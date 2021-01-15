/****************************************************************************
*
*  FileSourceTask.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.GZIPInputStream;

public abstract class FileSourceTask extends SourceTask
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(FileSourceTask.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private File directory = null;
  private int pollMaxRecords;
  private Charset fileCharset = null;
  private String topic = null;
  private String bootstrapServers = null;
  private String internalTopic = null;
  private Map<String,String> recordTopics = null;
  private String errorTopic = null;
  private File archiveDirectory = null;
  private int taskNumber;

  //
  //  state
  //

  private volatile boolean stopRequested = false;
  private volatile boolean resetCurrentFile = false;
  private ConnectSerde<StringValue> stringValueSerde = StringValue.serde();
  private KafkaConsumer<byte[], byte[]> consumer = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public int getTaskNumber() { return taskNumber; }
  protected String getCurrentFilename() { return currentFile != null ? currentFile.getName() : "unknown"; }
  protected boolean getStopRequested() { return stopRequested; }
  protected String getRecordTopic(String recordType) { return recordTopics.get(recordType); }

  /****************************************
  *
  *  attributes
  *
  ****************************************/

  //
  //  used only within poll/nextSourceRecords (i.e., no synchronization)
  //

  private List<SourceRecord> holdForNextPoll = new ArrayList<SourceRecord>();
  private volatile SourceFile currentFile = null;
  private BufferedReader reader = null;
  private long lineNumberWithData = 0L;
  private long lineNumber = 0L;

  //
  //  file lifecycle (including currentFile above)
  //

  private List<SourceFile> filesToProcess = Collections.<SourceFile>synchronizedList(new LinkedList<SourceFile>());
  private Set<SourceFile> processedFiles = Collections.<SourceFile>synchronizedSet(new HashSet<SourceFile>());

  //
  //  commit offsets
  //

  private Map<TopicPartition,Pair<Long,TreeSet<Long>>> partitionOffsets = new HashMap<TopicPartition,Pair<Long,TreeSet<Long>>>();
  private List<Map<TopicPartition,OffsetAndMetadata>> offsetsToBeCommitted = Collections.<Map<TopicPartition,OffsetAndMetadata>>synchronizedList(new LinkedList<Map<TopicPartition,OffsetAndMetadata>>());

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  protected abstract List<KeyValue> processRecord(String record) throws FileSourceTaskException;

  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return FileSourceConnector.FileSourceVersion;
  }

  /****************************************
  *
  *  start
  *
  ****************************************/

  @Override public void start(Map<String, String> taskConfig)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- Task.start() START", taskConfig.get("connectorName"));

    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  simple
    //

    connectorName = taskConfig.get("connectorName");
    directory = new File(taskConfig.get("directory"));
    pollMaxRecords = parseIntegerConfig(taskConfig.get("pollMaxRecords"));
    fileCharset = Charset.forName(taskConfig.get("fileCharset"));
    topic = taskConfig.get("topic");
    bootstrapServers = taskConfig.get("bootstrapServers");
    internalTopic = taskConfig.get("internalTopic");
    archiveDirectory = (taskConfig.get("archiveDirectory").trim().length() > 0) ? new File(taskConfig.get("archiveDirectory")) : null;
    taskNumber = parseIntegerConfig(taskConfig.get("taskNumber"));

    //
    //  recordTopics
    //  - format is "record1:topic1,record2:topic2,..."
    //

    log.info("topic {}", topic);
    recordTopics = new HashMap<String,String>();
    if (topic.contains(":"))
      {
        //
        //  extract each record/topic pair
        //

        String[] recordTopicTokens;
        try
          {
            recordTopicTokens = topic.split("[,]");
          }
        catch (PatternSyntaxException e)
          {
            throw new ServerRuntimeException(e);
          }

        //
        //  split single record and topic, updating recordTopics
        //

        for (String recordTopicToken : recordTopicTokens)
          {
            log.info("processing {}", recordTopicToken);
            String[] recordTopicPair;
            try
              {
                recordTopicPair = recordTopicToken.split("[:]", 2);
              }
            catch (PatternSyntaxException e)
              {
                throw new ServerRuntimeException(e);
              }
            if (recordTopicPair.length > 0)
              {
                log.info("recordTopicPair: ", recordTopicPair);
                recordTopics.put(recordTopicPair[0], recordTopicPair[1]);
              }
          }

        //
        //  topic
        //

        topic = null;
      }

    //
    //  errorTopic
    //

    errorTopic = recordTopics.get("error");

    /*****************************************
    *
    *  consumer
    *
    *****************************************/

    //
    // set up consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapServers);
    consumerProperties.put("group.id", "fileconnector-" + connectorName);
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
    ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
    {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { rebalanceRequested(partitions); }
      @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
    };
    consumer.subscribe(Arrays.asList(internalTopic), listener);

    //
    //  runConsumer
    //

    Runnable runConsumer = new Runnable() { @Override public void run() { runConsumer(); } };
    Thread runConsumerThread = new Thread(runConsumer, "RunConsumer");
    runConsumerThread.start();
  }

  /*****************************************
  *
  *  stop
  *
  *****************************************/

  @Override public void stop()
  {
    /*****************************************
    *
    *  mark stopRequested
    *
    *****************************************/

    stopRequested = true;

    /*****************************************
    *
    *  stop statistics
    *
    *****************************************/

    stopStatisticsCollection();
    
    /*****************************************
    *
    *  wake sleeping poll (if necessary)
    *
    *****************************************/

    consumer.wakeup();

    /*****************************************
    *
    *  wake sleeping poll (if necessary)
    *
    *****************************************/

    synchronized (this)
      {
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  poll
  *
  *****************************************/

  @Override public List<SourceRecord> poll() throws InterruptedException
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- poll START", connectorName);

    /*****************************************
    *
    *  handle held records
    *
    *****************************************/

    List<SourceRecord> result = new ArrayList<SourceRecord>(pollMaxRecords);
    result.addAll(holdForNextPoll);
    holdForNextPoll.clear();

    /*****************************************
    *
    *  poll
    *
    *****************************************/

    while (! stopRequested && result.size() < pollMaxRecords && holdForNextPoll.size() == 0)
      {
        /*****************************************
        *
        *  poll for new records
        *
        *****************************************/

        List<SourceRecord> sourceRecords = nextSourceRecords();

        /*****************************************
        *
        *  handle new sourceRecords
        *
        *****************************************/

        if (sourceRecords != null && result.size() + sourceRecords.size() <= pollMaxRecords)
          {
            result.addAll(sourceRecords);
          }
        else if (sourceRecords != null)
          {
            holdForNextPoll.addAll(sourceRecords);
          }

        /*****************************************
        *
        *  resetCurrentFile (if a rebalance has occurred)
        *
        *****************************************/

        if (resetCurrentFile)
          {
            // 
            //  ignore sourceRecords
            //

            sourceRecords = null;

            //
            //  clear any records already in result
            //

            result.clear();

            //
            //  clear holdForNextPoll
            //

            holdForNextPoll.clear();
            
            //
            //  clear currentFile
            //

            if (currentFile != null)
              {
                //
                //  close reader (if necessary)
                //

                if (reader != null)
                  {
                    try
                      {
                        reader.close();
                      }
                    catch (IOException e)
                      {
                        // ignore
                      }
                  }

                //
                //  clear currentFile
                //

                currentFile = null;
                reader = null;
                lineNumberWithData = 0L;
                lineNumber = 0L;
              }

            //
            //  reset
            //

            resetCurrentFile = false;
          }

        /*****************************************
        *
        *  break if no more data and there is a result
        *
        *****************************************/

        if (sourceRecords == null && result.size() > 0)
          {
            break;
          }

        /*****************************************
        *
        *  sleep if no more data and there is no result
        *
        *****************************************/

        if (sourceRecords == null && result.size() == 0)
          {
            synchronized (this)
              {
                while (! stopRequested && filesToProcess.size() == 0)
                  {
                    try
                      {
                        this.wait();
                      }
                    catch (InterruptedException e)
                      {
                        // ignore
                      }
                  }
              }
          }
      }

    /*****************************************
    *
    *  log
    *
    *****************************************/

    //
    //  poll
    //

    log.debug("{} -- poll returning {} records", connectorName, result.size());

    //
    //  stopTask
    //

    if (stopRequested)
      {
        log.info("{} -- Task.stop()", connectorName);
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /*****************************************
  *
  *  nextSourceRecords
  *
  *****************************************/

  private List<SourceRecord> nextSourceRecords()
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    List<SourceRecord> result = null;

    /*****************************************
    *
    *  loop
    *
    *****************************************/

    while (true)
      {
        /*****************************************
        *
        *  read next record from currentFile
        *
        *****************************************/

        if (currentFile != null)
          {
            /*****************************************
            *
            *  read next record
            *
            *****************************************/

            String record = null;
            try
              {
                record = reader.readLine();
                if (record != null) lineNumber += 1;
              }
            catch (IOException e)
              {
                log.info("bufferedReader", e);
                record = null;
              }

            /*****************************************
            *
            *  end-of-file -- continue if EOF
            *
            *****************************************/

            if (record == null)
              {
                //
                //  close reader
                //

                try
                  {
                    reader.close();
                  }
                catch (IOException e)
                  {
                    // ignore
                  }

                //
                //  close currentFile
                //

                currentFile.markProcessed(lineNumberWithData);
                processedFiles.add(currentFile);
                currentFile = null;
                reader = null;
                lineNumberWithData = 0L;
                lineNumber = 0L;

                //
                // statistics
                //

                updateFilesProcessed(connectorName, taskNumber, 1);

                //
                //  continue
                //

                continue;
              }

            /*****************************************
            *
            *  filter blank lines -- continue if blank
            *
            *****************************************/

            if (record.trim().length() == 0)
              {
                continue;
              }

            /*****************************************
            *
            *  parse
            *
            *****************************************/

            List<KeyValue> recordResults = null;
            try
              {
                recordResults = processRecord(record.trim());
              }
            catch (FileSourceTaskException e)
              {
                //
                // statistics
                //

                updateErrorRecords(connectorName, taskNumber, 1);

                //
                // error record
                //

                if (errorTopic != null)
                  {
                    ErrorRecord errorRecord = new ErrorRecord(record.trim(), SystemTime.getCurrentTime(), currentFile.getName(), e.getMessage());
                    recordResults = Collections.<KeyValue>singletonList(new KeyValue("error", null, null, ErrorRecord.schema(), ErrorRecord.pack(errorRecord)));
                  }
                else
                  {
                    continue;
                  }
              }

            /*****************************************
            *
            *  update lineNumberWithData (for commit/restart)
            *
            *****************************************/

            lineNumberWithData = (recordResults.size() > 0) ? lineNumber : lineNumberWithData;
            
            /*****************************************
            *
            *  build SourceRecords
            *
            *****************************************/

            result = new ArrayList<SourceRecord>();
            long recordNumber = 0;
            for (KeyValue recordResult : recordResults)
              {
                //
                //  topic
                //

                String recordTopic = (recordResult.getRecordType() != null) ? recordTopics.get(recordResult.getRecordType()) : topic;
                if (recordTopic == null) throw new ServerRuntimeException("cannot determine topic for record type " + recordResult.getRecordType());

                //
                //  sourcePartition
                //

                Map<String, String> sourcePartition = new HashMap<String, String>();
                sourcePartition.put("absolutePath", currentFile.getAbsolutePath());

                //
                //  sourceOffset
                //

                Map<String, Long> sourceOffset = new HashMap<String, Long>();
                sourceOffset.put("lineNumber", lineNumberWithData);
                sourceOffset.put("recordNumber", recordNumber);
                sourceOffset.put("totalRecords", (long) recordResults.size());

                //
                //  sourceRecord
                //

                result.add(new SourceRecord(sourcePartition, sourceOffset, recordTopic, recordResult.getKeySchema(), recordResult.getKey(), recordResult.getValueSchema(), recordResult.getValue()));

                //
                //  increment recordNumber
                //

                recordNumber += 1;

                //
                //  statistics
                //

                updateRecordStatistics(connectorName, taskNumber, recordTopic, 1);
              }

            /*****************************************
            *
            *  found record -- break and return
            *
            *****************************************/

            break;
          }

        /*****************************************
        *
        *  next file from filesToProcess -- continue if found
        *  -- initialize currentFile, lineNumber, lineNumberWithData, and reader as necessary
        *
        *****************************************/

        //
        //  get fileToProcess (if any)
        //

        SourceFile fileToProcess = null;
        synchronized (filesToProcess)
          {
            fileToProcess = (filesToProcess.size() > 0) ? filesToProcess.remove(0) : null;
          }

        //
        //  fileToProcess
        //

        if (fileToProcess != null)
          {
            /****************************************
            *
            *  initialize currentFile
            *
            ****************************************/

            //
            //  get first fileToProcess
            //

            currentFile = fileToProcess;

            //
            //  log
            //

            log.debug("{} -- fileToProcess: {}", connectorName, currentFile.getAbsolutePath());

            //
            //  ensure currentFile still exists
            //

            if (! currentFile.exists())
              {
                currentFile.markProcessed(null);
                processedFiles.add(currentFile);
                currentFile = null;
                continue;
              }

            //
            //  reader  
            //

            try
              {
                /****************************************
                *
                *  initialize lineNumber and lineNumberWithData
                *
                ****************************************/

                //
                //  retrieve lineNumber from source offset (to handle restart case)
                //

                Map<String, String> sourcePartition = new HashMap<String, String>();
                sourcePartition.put("absolutePath", currentFile.getAbsolutePath());
                Map<String, Object> sourceOffset = context.offsetStorageReader().offset(sourcePartition);
                Long restartLineNumber = (sourceOffset != null) ? (Long) sourceOffset.get("lineNumber") : null;
                Long restartRecordNumber = (sourceOffset != null) ? (Long) sourceOffset.get("recordNumber") : null;
                Long restartTotalRecords = (sourceOffset != null) ? (Long) sourceOffset.get("totalRecords") : null;

                //
                //  case - file not yet seen
                //

                if (restartLineNumber == null || restartRecordNumber == null || restartTotalRecords == null)
                  {
                    log.debug("fileToProcess (not seen) {}", currentFile.getAbsolutePath());
                    lineNumberWithData = 0L;
                    lineNumber = 0L;
                  }

                //
                //  case - first line in file not fully processed
                //

                else if (restartLineNumber.longValue() == 1L && restartRecordNumber.longValue() != restartTotalRecords.longValue()-1L)
                  {
                    log.info("fileToProcess (first line not fully processed) {}", currentFile.getAbsolutePath());
                    lineNumberWithData = 0L;
                    lineNumber = 0L;
                  }

                //
                //  case - file in progress
                //

                else
                  {
                    log.info("fileToProcess (restart) {}", currentFile.getAbsolutePath());
                    lineNumberWithData = (restartRecordNumber.longValue() == restartTotalRecords.longValue()-1L) ? restartLineNumber : new Long(restartLineNumber.longValue()-1L);
                    lineNumber = lineNumberWithData;
                  }

                /****************************************
                *
                *  initialize reader
                *
                ****************************************/

                //
                //  gzip format?
                //

                Pattern gzipPattern = Pattern.compile("^.*\\.(.*)$");
                Matcher gzipMatcher = gzipPattern.matcher(currentFile.getAbsolutePath());
                boolean gzipFormat = gzipMatcher.matches() && gzipMatcher.group(1).trim().toLowerCase().equals("gz");

                //
                //  input stream w/ gzip support
                //

                FileInputStream fileInputStream = new FileInputStream(currentFile.getAbsolutePath());
                InputStream inputStream = gzipFormat ? new GZIPInputStream(fileInputStream) : fileInputStream;

                //
                //  restart reader
                //

                reader = new BufferedReader(new InputStreamReader(inputStream, fileCharset));
                boolean eofReached = false;
                for (long i = 0; i < lineNumber; i++)
                  {
                    String record = reader.readLine();
                    if (record == null)
                      {
                        eofReached = true;
                        break;
                      }
                  }

                //
                //  check for end-of-file with one more read
                //

                if (!eofReached)
                  {
                    reader.mark(1);
                    if (reader.read() == -1)
                      {
                        eofReached = true;
                      }
                    else
                      {
                        reader.reset();
                      }
                  }

                //
                //  cleanup (if necessary)
                //

                if (eofReached)
                  {
                    //
                    //  close reader
                    //

                    try
                      {
                        reader.close();
                      }
                    catch (IOException e)
                      {
                        // ignore
                      }

                    //
                    //  close currentFile
                    //

                    currentFile.markProcessed(lineNumberWithData);
                    processedFiles.add(currentFile);
                    currentFile = null;
                    reader = null;
                    lineNumberWithData = 0L;
                    lineNumber = 0L;
                  }
              }
            catch (IOException e)
              {
                log.info("bufferedReader", e);
                if (reader != null)
                  {
                    try
                      {
                        reader.close();
                      }
                    catch (IOException e1)
                      {
                        // ignore
                      }
                  }
                currentFile.markProcessed(lineNumberWithData);
                processedFiles.add(currentFile);
                currentFile = null;
                reader = null;
                lineNumberWithData = 0L;
                lineNumber = 0L;
              }

            //
            //  continue
            //

            continue;
          }

        /*****************************************
        *
        *  no result and no files available -- break and return
        *
        *****************************************/

        break;
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /*****************************************
  *
  *  commit
  *
  *****************************************/

  //
  //  commitLock
  //

  private Object commitLock = new Object();

  //
  //  commit
  //

  @Override public void commit()
  {
    synchronized (commitLock)
      {
        if (! stopRequested)
          {
            /*****************************************
            *
            *  log
            *
            *****************************************/

            log.trace("{} -- commit START", connectorName);

            /*****************************************
            *
            *  candidateFiles
            *
            *****************************************/

            Set<SourceFile> candidateFiles;
            synchronized (processedFiles)
              {
                candidateFiles = new HashSet<SourceFile>(processedFiles);
              }

            /*****************************************
            *
            *  delete files as necessary
            *
            *****************************************/

            Set<SourceFile> completedFiles = new HashSet<SourceFile>();
            for (SourceFile sourceFile : candidateFiles)
              {
                /*****************************************
                *
                *  verify -- completely processed?
                *
                *****************************************/

                //
                //  retrieve lineNumber from source offset
                //

                Map<String, String> sourcePartition = new HashMap<String, String>();
                sourcePartition.put("absolutePath", sourceFile.getAbsolutePath());
                Map<String, Object> sourceOffset = context.offsetStorageReader().offset(sourcePartition);
                Long commitedLineNumber = (sourceOffset != null) ? (Long) sourceOffset.get("lineNumber") : null;
                Long committedRecordNumber = (sourceOffset != null) ? (Long) sourceOffset.get("recordNumber") : null;
                Long committedTotalRecords = (sourceOffset != null) ? (Long) sourceOffset.get("totalRecords") : null;

                //
                //  verify 
                //

                Long lastLineNumber = sourceFile.getLineNumberWithData();
                if ((lastLineNumber != null && lastLineNumber > 0L) && (commitedLineNumber == null || committedRecordNumber == null || committedTotalRecords == null || commitedLineNumber.longValue() != lastLineNumber.longValue() || committedRecordNumber.longValue() != committedTotalRecords.longValue()-1L))
                  {
                    continue;
                  }

                /*****************************************
                *
                *  delete/archive
                *
                *****************************************/

                if (sourceFile.exists())
                  {
                    try
                      {
                        boolean successfullyRemoved = (archiveDirectory != null) ? sourceFile.getFile().renameTo(new File(archiveDirectory,sourceFile.getName())) :  sourceFile.getFile().delete();
                        if (!successfullyRemoved)
                          {
                            log.error("COULD NOT REMOVE FILE {}", sourceFile.getAbsolutePath());
                          }
                      }
                    catch (SecurityException e)
                      {
                        log.error("COULD NOT REMOVE FILE {}: {}", sourceFile.getAbsolutePath(), e.getMessage());
                      }
                  }

                /*****************************************
                *
                *  completed files
                *
                *****************************************/

                completedFiles.add(sourceFile);
              }

            /*****************************************
            *
            *  reset processedFiles
            *
            *****************************************/

            synchronized (processedFiles)
              {
                for (SourceFile completedFile : completedFiles)
                  {
                    processedFiles.remove(completedFile);
                  }
              }

            /*****************************************
            *
            *  commit offsets?
            *
            *****************************************/

            synchronized (partitionOffsets)
              {
                //
                // add "offset to be committed" to partitionOffsets and remove from fileOffsets
                //

                for (SourceFile completedFile : completedFiles)
                  {
                    TopicPartition partition = completedFile.getPartition();
                    long offset = completedFile.getOffset();
                    if (partitionOffsets.get(partition) != null)
                      {
                        partitionOffsets.get(partition).getSecondElement().add(offset);
                      }
                  }

                //
                //  partitionOffsets -- commit?
                //

                Map<TopicPartition,OffsetAndMetadata> offsetsToBeCommitted = new HashMap<TopicPartition,OffsetAndMetadata>();
                for (TopicPartition partition : partitionOffsets.keySet())
                  {
                    long currentCommittedOffset = partitionOffsets.get(partition).getFirstElement();
                    long offsetToCommit = currentCommittedOffset;
                    Iterator<Long> commitableOffsets = partitionOffsets.get(partition).getSecondElement().iterator();
                    while (commitableOffsets.hasNext())
                      {
                        long commitableOffset = commitableOffsets.next();
                        if (commitableOffset == offsetToCommit + 1L)
                          {
                            offsetToCommit = commitableOffset;
                            commitableOffsets.remove();
                          }
                        else
                          {
                            break;
                          }
                      }
                    if (offsetToCommit > currentCommittedOffset)
                      {
                        offsetsToBeCommitted.put(partition, new OffsetAndMetadata(offsetToCommit + 1L));
                        partitionOffsets.put(partition, new Pair<Long,TreeSet<Long>>(offsetToCommit, partitionOffsets.get(partition).getSecondElement()));
                      }
                  }

                //
                //  commit
                //

                if (offsetsToBeCommitted.size() > 0)
                  {
                    this.offsetsToBeCommitted.add(offsetsToBeCommitted);
                  }
              }
          }
      }
  }

  /*****************************************
  *
  *  runConsumer
  *
  *****************************************/

  private void runConsumer()
  {
    /*****************************************
    *
    *  run
    *
    *****************************************/

    while (! stopRequested)
      {
        //
        //  poll
        //

        ConsumerRecords<byte[], byte[]> records;
        try
          {
            records = consumer.poll(1000);
          }
        catch (WakeupException e)
          {
            records = ConsumerRecords.<byte[], byte[]>empty();
          }

        //
        //  update filesToProcess
        //

        
        for (ConsumerRecord<byte[], byte[]> record : records)
          {
            //
            //  next file
            //

            SourceFile sourceFile = new SourceFile(record);

            //
            //  fileToProcess
            //

            synchronized (this)
              {
                filesToProcess.add(sourceFile);
                this.notifyAll();
              }

            //
            //  partitionOffsets
            //

            synchronized (partitionOffsets)
              {
                if (partitionOffsets.get(sourceFile.getPartition()) == null)
                  {
                    partitionOffsets.put(sourceFile.getPartition(), new Pair<Long,TreeSet<Long>>(sourceFile.getOffset() - 1, new TreeSet<Long>()));
                  }
              }
          }

        //
        //  commit
        //

        Map<TopicPartition,OffsetAndMetadata> offsets = new HashMap<TopicPartition,OffsetAndMetadata>();
        while (offsetsToBeCommitted.size() > 0)
          {
            Map<TopicPartition,OffsetAndMetadata> offsetToBeCommitted = offsetsToBeCommitted.remove(0);
            if (offsetToBeCommitted.size() > 0)
              {
                offsets.putAll(offsetToBeCommitted);
              }
          }
        if (offsets.size() > 0) consumer.commitSync(offsets);
      }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    consumer.close();
  }

  /*****************************************
  *
  *  rebalanceRequested
  *
  *****************************************/
  
  private void rebalanceRequested(Collection<TopicPartition> revokedPartitions)
  {
    //
    //  current file
    //

    SourceFile currentFile = this.currentFile;
    if (currentFile != null && revokedPartitions.contains(currentFile.getPartition()))
      {
        resetCurrentFile = true;
      }

    //
    //  filesToProcess
    //

    synchronized (filesToProcess)
      {
        Iterator<SourceFile> filesToProcessIterator = filesToProcess.iterator();
        while (filesToProcessIterator.hasNext())
          {
            SourceFile sourceFile = filesToProcessIterator.next();
            if (revokedPartitions.contains(sourceFile.getPartition()))
              {
                filesToProcessIterator.remove();
              }
          }
      }

    //
    //  commit
    //

    commit();

    //
    //  partitionOffsets
    //

    synchronized (partitionOffsets)
      {
        //
        //  partitionOffsets
        //

        for (TopicPartition partition : revokedPartitions)
          {
            partitionOffsets.remove(partition);
          }
      }

    //
    //  offsetsToBeCommitted
    //

    synchronized (offsetsToBeCommitted)
      {
        for (Map<TopicPartition,OffsetAndMetadata> offsetsToBeCommitted : this.offsetsToBeCommitted)
          {
            for (TopicPartition partition : revokedPartitions)
              {
                offsetsToBeCommitted.remove(partition);
              }
          }
      }
  }

  /*****************************************
  *
  *  class SourceFile
  *
  *****************************************/

  private class SourceFile
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private File file;
    private TopicPartition partition;
    private long offset;
    private Long lineNumberWithData;
    private boolean processed;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    private SourceFile(ConsumerRecord<byte[],byte[]> record)
    {
      StringValue filename = stringValueSerde.deserializer().deserialize(record.topic(), record.value());
      this.file = new File(directory, filename.getValue());
      this.partition = new TopicPartition(record.topic(), record.partition());
      this.offset = record.offset();
      this.lineNumberWithData = null;
      this.processed = false;
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public File getFile() { return file; }
    public TopicPartition getPartition() { return partition; }
    public long getOffset() { return offset; }
    public Long getLineNumberWithData() { return lineNumberWithData; }
    public boolean getProcessed() { return processed; }

    //
    //  computed
    //

    public String getName() { return file.getName(); }
    public String getAbsolutePath() { return file.getAbsolutePath(); }
    public boolean exists() { return file.exists(); }

    /*****************************************
    *
    *  setters
    *
    *****************************************/

    public void markProcessed(Long lineNumberWithData)
    {
      this.processed = true;
      this.lineNumberWithData = lineNumberWithData;
    }
  }

  /*****************************************
  *
  *  statistics
  *    - updateFilesProcessed
  *    - updateErrorRecords
  *    - updateRecordStatistics
  *
  *****************************************/

  //
  //  getStatistics
  //

  private Map<Integer,FileSourceTaskStatistics> allTaskStatistics = new HashMap<Integer, FileSourceTaskStatistics>();
  private FileSourceTaskStatistics getStatistics(String connectorName, int taskNumber)
  {
    synchronized (allTaskStatistics)
      {
        FileSourceTaskStatistics taskStatistics = allTaskStatistics.get(taskNumber);
        if (taskStatistics == null)
          {
            try
              {
                taskStatistics = new FileSourceTaskStatistics(connectorName, taskNumber);
              }
            catch (ServerException se)
              {
                throw new ServerRuntimeException("Could not create statistics object", se);
              }
            allTaskStatistics.put(taskNumber, taskStatistics);
          }
        return taskStatistics;
      }
  }    

  //
  //  updateFilesProcessed
  //

  private void updateFilesProcessed(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateFilesProcessed(amount);
      }
  }

  //
  //  updateErrorRecords
  //

  private void updateErrorRecords(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateErrorRecords(amount);
      }
  }

  //
  //  updateRecordStatistics
  //

  private void updateRecordStatistics(String connectorName, int taskNumber, String recordType, int amount)
  {
    synchronized (allTaskStatistics)
      {
        FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateRecordStatistics(recordType, amount);
      }
  }

  //
  //  stopStatisticsCollection
  //

  private void stopStatisticsCollection()
  {
    synchronized (allTaskStatistics)
      {
        for (FileSourceTaskStatistics taskStatistics : allTaskStatistics.values())
          {
            taskStatistics.unregister();
          }
        allTaskStatistics.clear();
      }
  }

  /*****************************************
  *
  *  class KeyValue
  *
  *****************************************/

  public static class KeyValue
  {
    private String recordType;
    private Schema keySchema;
    private Object key;
    private Schema valueSchema;
    private Object value;

    //
    //  constructor
    //

    public KeyValue(String recordType, Schema keySchema, Object key, Schema valueSchema, Object value)
    {
      this.recordType = recordType;
      this.keySchema = keySchema;
      this.key = key;
      this.valueSchema = valueSchema;
      this.value = value;
    }

    //
    //  constructor
    //

    public KeyValue(Schema keySchema, Object key, Schema valueSchema, Object value)
    {
      this.recordType = null;
      this.keySchema = keySchema;
      this.key = key;
      this.valueSchema = valueSchema;
      this.value = value;
    }

    //
    //  accessors
    //

    public String getRecordType() { return recordType; }
    public Schema getKeySchema() { return keySchema; }
    public Object getKey() { return key; }
    public Schema getValueSchema() { return valueSchema; }
    public Object getValue() { return value; }
  }

  /*****************************************
  *
  *  class FileSourceTaskException
  *
  *****************************************/

  public static class FileSourceTaskException extends Exception
  {       
    public FileSourceTaskException(String message) { super(message); }
    public FileSourceTaskException(Throwable e) { super(e); }
    public FileSourceTaskException(String message, Throwable e) { super(message,e); }
  }

  /*****************************************
  *
  *  parseIntegerConfig
  *
  *****************************************/

  protected Integer parseIntegerConfig(String attribute)
  {
    try
      {
        return (attribute != null) ? Integer.parseInt(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        return null;
      }
  }

  /****************************************
  *
  *  readString
  *
  ****************************************/

  protected String readString(String token, String record, String defaultValue) { return FileSourceConnector.readString(token, record, defaultValue); }
  protected String readString(String token, String record) { return FileSourceConnector.readString(token, record, null); }
  
  /****************************************
  *
  *  readInteger
  *
  ****************************************/

  protected Integer readInteger(String token, String record, Integer defaultValue) throws IllegalArgumentException { return FileSourceConnector.readInteger(token, record, defaultValue); }
  protected Integer readInteger(String token, String record) throws IllegalArgumentException { return FileSourceConnector.readInteger(token, record, null); }
  
  /****************************************
  *
  *  readLong
  *
  ****************************************/

  protected Long readLong(String token, String record, Long defaultValue) throws IllegalArgumentException { return FileSourceConnector.readLong(token, record, defaultValue); }
  protected Long readLong(String token, String record) throws IllegalArgumentException { return FileSourceConnector.readLong(token, record, null); }

  /****************************************
  *
  *  readBoolean
  *
  ****************************************/

  protected Boolean readBoolean(String token, String record, Boolean defaultValue) throws IllegalArgumentException { return FileSourceConnector.readBoolean(token, record, defaultValue); }
  protected Boolean readBoolean(String token, String record) throws IllegalArgumentException { return FileSourceConnector.readBoolean(token, record, null); }

  /****************************************
  *
  *  readDate
  *
  ****************************************/

  protected Date readDate(String token, String record, String format, String timeZone, Date defaultValue) throws IllegalArgumentException { return FileSourceConnector.readDate(token, record, format, timeZone, defaultValue); }
  protected Date readDate(String token, String record, String format, String timeZone) throws IllegalArgumentException { return FileSourceConnector.readDate(token, record, format, timeZone, null); }
  protected Date readDate(String token, String record, String format, Date defaultValue, int tenantID) throws IllegalArgumentException { return FileSourceConnector.readDate(token, record, format, Deployment.getDeployment(tenantID).getBaseTimeZone(), defaultValue); }
  protected Date readDate(String token, String record, String format, int tenantID) throws IllegalArgumentException { return FileSourceConnector.readDate(token, record, format, Deployment.getDeployment(tenantID).getBaseTimeZone(), null); }

  /****************************************
  *
  *  readDouble
  *
  ****************************************/

  protected Double readDouble(String token, String record, Double defaultValue) throws IllegalArgumentException { return FileSourceConnector.readDouble(token, record, defaultValue); }
  protected Double readDouble(String token, String record) throws IllegalArgumentException { return FileSourceConnector.readDouble(token, record, null); }
}
