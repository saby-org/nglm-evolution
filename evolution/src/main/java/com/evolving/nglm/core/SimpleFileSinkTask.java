/****************************************************************************
*
*  class SimpleFileSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SimpleFileSinkTask extends SinkTask
{
  /*****************************************
  *
  *  static
  *
  *****************************************/
  
  private static String TEMP_FILE_PREFIX = "temp_"; // NO NOT USE ANY REGEX IN THIS STRING
  private static int ROTATION_INDEX_PADDING = 3;    // length of 0-padding for rotation index : 001, 002, ...
  private static final String ROTATION_INDEX_PATTERN = "ROTATE_INDEX";

  /*****************************************
  *
  *  config
  *
  *****************************************/
  
  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleFileSinkTask.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String fileNamePattern = SimpleFileSinkConnector.DEFAULT_FILE_NAME_PATTERN;
  private String rootDir = SimpleFileSinkConnector.DEFAULT_ROOT_DIR;
  private String fileExtension = SimpleFileSinkConnector.DEFAULT_FILE_EXTENSION;
  private int maxFileSize = SimpleFileSinkConnector.DEFAULT_MAX_FILE_SIZE;
  private int maxTimeMinutes = SimpleFileSinkConnector.DEFAULT_MAX_TIME_MINUTES;
  private int maxNumRecords = SimpleFileSinkConnector.DEFAULT_MAX_NUM_RECORDS;
  private int taskNumber;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public int getTaskNumber() { return taskNumber; }

  /****************************************
  *
  *  attributes
  *
  ****************************************/

  private PrintStream pStream = null;
  private String currentFileName;
  private int bytesWritten = 0;
  private int lengthEndOfLine = 2;
  private int recordsWritten = 0;
  private Calendar calendar = new GregorianCalendar();
  private String filenamePrefix = "";
  private long tsFileLimit;
  private int currentRotationIndex = 0;

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract String extractLine(SinkRecord sinkRecord);
  
  /*****************************************
  *
  *  version
  *
  *****************************************/

  @Override public String version()
  {
    return SimpleFileSinkConnector.SimpleFileSinkVersion;
  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  @Override public void start(Map<String, String> taskConfig)
  {
    try
      {
        /*****************************************
        *
        *  log
        *
        *****************************************/
        
        connectorName = taskConfig.get("connectorName");
        log.info("{} -- Task.start() START", connectorName);

        /*****************************************
        *
        *  config
        *
        *****************************************/

        fileNamePattern = taskConfig.get("fileNamePattern");
        rootDir = taskConfig.get("rootDir");
        fileExtension = taskConfig.get("fileExtension");
        maxFileSize = SimpleFileSinkConnector.parseIntegerConfig(taskConfig.get("maxFileSize"));
        maxTimeMinutes = SimpleFileSinkConnector.parseIntegerConfig(taskConfig.get("maxTimeMinutes"));
        maxNumRecords = SimpleFileSinkConnector.parseIntegerConfig(taskConfig.get("maxNumRecords"));
        taskNumber = SimpleFileSinkConnector.parseIntegerConfig(taskConfig.get("taskNumber"));
        log.info("{} -- fileNamePattern: {}, rootDir: {}, fileExtension: {}, maxFileSize: {}, maxTimeMinutes: {}, maxNumRecords: {}, taskNumber: {}", connectorName, fileNamePattern, rootDir, fileExtension, maxFileSize, maxTimeMinutes, maxNumRecords, taskNumber);

        /*****************************************
        *
        *  initialize client
        *
        *****************************************/

        //
        //  initialize filename
        //
        
        computeInitialFilename();
        
        /*****************************************
        *
        *  log
        *
        *****************************************/

        log.info("{} -- Task.start() END", connectorName);
      }
    catch (Exception e)
      {
        log.info("{} -- Task.start() EXCEPTION : {}", connectorName, e.getLocalizedMessage());
      }
  }

  /*****************************************
  *
  *  computeInitialFilename
  *
  *****************************************/

  private void computeInitialFilename()
  {
    calendar.setTime(SystemTime.getCurrentTime()); // set date once and for all
    computeNewFilename();
  }
  
  /*****************************************
  *
  *  computeNewFilename
  *
  *****************************************/

  private void computeNewFilename()
  {
    long ts = currentTimeStampSecond();
    int countPercent = fileNamePattern.length() - fileNamePattern.replace("%", "").length();
    Calendar[] ca = new Calendar[countPercent];
    for (int i=0; i<countPercent; i++)
      {
        //
        //  We do not change the date in calendar (we use the date when the file was first created)
        //
        
        ca[i] = calendar;
      }
    filenamePrefix  = String.format(fileNamePattern, ca); // vararg simulated with array
    log.debug("Using filenamePrefix : " + filenamePrefix);
    bytesWritten = 0;
    recordsWritten = 0;

    //
    //  maxTime is in minutes
    //

    tsFileLimit = currentTimeStampSecond() + maxTimeMinutes * 60;
    currentRotationIndex ++;
    String actualFilename = filenamePrefix.replaceFirst(ROTATION_INDEX_PATTERN, String.format("%0"+ROTATION_INDEX_PADDING+"d", currentRotationIndex));
    currentFileName = rootDir + "/" + TEMP_FILE_PREFIX + actualFilename + "." + ts + "." + taskNumber + "." + fileExtension;
    log.debug("Using currentFileName : "+ currentFileName);
  }
  
  /*****************************************
  *
  *  currentTimeStampSecond
  *
  *****************************************/
  
  private long currentTimeStampSecond()
  {
    return (long) (System.currentTimeMillis()/1000);
  }
  
  /*****************************************
  *
  *  checkTempFilesForPromotion
  *
  *****************************************/

  public static void promoteTempFiles(String rootDir)
  {
    log.info("Promoting temp files in "+ rootDir);
    for (String fn : new File(rootDir).list())
      {
        log.debug("Checking file " + fn);
        if (fn.startsWith(TEMP_FILE_PREFIX))
          {
          String fnFullPath = rootDir + "/" + fn;
          log.debug("File may remain from a crash, promote it : " + fnFullPath);
          promoteFile(fnFullPath);
          }
      }
  }

  /*****************************************
  *
  *  promoteFile
  *
  *****************************************/

  private static void promoteFile(String fn)
  {
    // if we didn't get any put() since last rotate(), fn might not exist. In this case, do nothing.
    if (new File(fn).exists())
    {
      String newFileName = fn.replaceFirst(TEMP_FILE_PREFIX, "");
      log.debug("Trying to rename "+ fn +" to " + newFileName);
      File newFile = new File(newFileName);
      if (newFile.exists())
      {
        String ts = currentTimeStampNanosecond();
        log.info("New file already exists, let's append a timestamp : "+ ts);
        newFileName = newFileName + "." + ts;
        newFile = new File(newFileName);
      }
      log.info("Renaming " + fn + " to "+ newFileName);
      if (!new File(fn).renameTo(newFile))
      {
        log.info("Failed to rename " + fn + " to "+ newFileName);
      }
    }
  }

  /*****************************************
  *
  *  currentTimeStampNanosecond
  *
  *****************************************/
  
  private static GregorianCalendar cal2 = new GregorianCalendar();
  private static String currentTimeStampNanosecond()
  {
    cal2.setTime(SystemTime.getCurrentTime());

    //
    // 's' Seconds since 1 January 1970 00:00:00 UTC, 'N' Nanosecond within the second, formatted as nine digits with leading zeros as necessary
    //
    
    String ts = String.format("%ts%tN", cal2, cal2);    
    return ts;
  }
    
  /*****************************************
  *
  *  put
  *
  *****************************************/

  @Override public void put(Collection<SinkRecord> sinkRecords)
  {
    /****************************************
    *
    *  insert all records
    *
    ****************************************/

    if (sinkRecords.isEmpty())
      log.trace("{} -- Task.put() - 0 records.", connectorName);
    else
      log.info("{} -- Task.put() - {} records.", connectorName, sinkRecords.size());
    
    for (SinkRecord sinkRecord : sinkRecords)
      {
        rotateIfNecessary();
        String line;
        try
          {
            line = extractLine(sinkRecord); // from subclass
          }
        catch (RuntimeException e)
          {
            throw new ConnectException("Error parsing sinkRecord", e);
          }
        if (line == null)
          {
            line = "";
          }
        if (pStream == null) // first time we write to this file
          {
            pStream = createFile(currentFileName);
          }
        log.trace("Writing " + line);
        if (pStream != null)
          {
            pStream.println(line);
            if (pStream.checkError())
              {
                throw new ConnectException("Error writing '" + line + "' to file " + currentFileName);
              }
            bytesWritten += line.length() + lengthEndOfLine;
            recordsWritten++;
            log.info("bytesWritten = " + bytesWritten + " , recordsWritten = " + recordsWritten);
          }
        else
          {
            log.info("Unexpected : pStream = null");
            throw new ConnectException("Error writing '" + line + "' to file " + currentFileName);
          }
      }

    //
    //  statistics
    //

    updateRecordCount(connectorName, taskNumber, sinkRecords.size());
    updatePutCount(connectorName, taskNumber, 1);
    log.info("{} -- Task.put() END", connectorName);
  }
  
  /*****************************************
  *
  *  createFile
  *
  *****************************************/

  private PrintStream createFile(String fn)
  {
    PrintStream ps = null;
    try
      {
        ps = new PrintStream(new FileOutputStream(fn), false, StandardCharsets.UTF_8.name());
      }
    catch (FileNotFoundException | UnsupportedEncodingException e)
      {
        log.info("Exception : " + e.getLocalizedMessage());
        throw new ConnectException("Error creating file " + fn + " : "+ e.getLocalizedMessage());
      }
    log.trace("Created ps : "+ ps);
    return ps;
  }

  /*****************************************
  *
  *   rotateIfNecessary
  *
  *****************************************/

  private void rotateIfNecessary()
  {
    if (shouldRotate()) 
      {
        rotate();
      }
  }
  
  /*****************************************
  *
  *   shouldRotate
  *
  *****************************************/

  private boolean shouldRotate()
  {
    long currentTs = currentTimeStampSecond();
    boolean res = ((bytesWritten >= maxFileSize) || (recordsWritten >= maxNumRecords) || (currentTs >= tsFileLimit));
    log.debug("shouldRotate() : comparing " + bytesWritten + " and " + maxFileSize + " and " + recordsWritten + " and " + maxNumRecords + " and " + currentTs + " and " + tsFileLimit + " result : " + res);
    return res;
  }
  
  /*****************************************
  *
  *   rotate
  *
  *****************************************/

  private void rotate()
  {
    if (pStream != null)
      {
        pStream.close();
        if (pStream.checkError())
          {
            log.info("Issue closing " + currentFileName);
            throw new ConnectException("Error creating file " + currentFileName);
          }
        pStream = null;
        promoteFile(currentFileName);
        computeNewFilename();
      }
  }
  
  /*****************************************
  *
  *  flush
  *
  *****************************************/

  @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map)
  {
    log.info("{} -- Task.flush() START", connectorName);
    if (pStream != null)
      {
        pStream.flush();
        if (pStream.checkError())
          {
            log.info("Issue flushing " + currentFileName);
            throw new ConnectException("Error flushing file " + currentFileName);
          }
        rotateIfNecessary();
      }
    log.info("{} -- Task.flush() END", connectorName);
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
    *  log
    *
    *****************************************/
    
    log.info("{} -- Task.stop() START", connectorName);
    
    /*****************************************
    *
    *  unregister statistics
    *
    *****************************************/
    
    stopStatisticsCollection();

    /*****************************************
    *
    *  shut down
    *
    *****************************************/
            
    rotate();
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("{} -- Task.stop() END", connectorName);
  }
  
  /****************************************************************************
  *
  *  statistics
  *    - updateBatchCount
  *    - updatePutCount
  *    - updateRecordCount
  *    - stopStatisticsCollection
  *
  ****************************************************************************/

  /*****************************************
  *
  *  statistics data (transient)
  *
  *****************************************/

  private String statisticsKey(String connector, int taskNumber) { return connector + "-" + taskNumber; }
  private static Map<String, com.evolving.nglm.core.SinkTaskStatistics> allTaskStatistics = new HashMap<String, com.evolving.nglm.core.SinkTaskStatistics>();

  /*****************************************
  *
  *  getStatistics
  *
  *****************************************/

  private com.evolving.nglm.core.SinkTaskStatistics getStatistics(String connectorName, int taskNumber)
  {
    synchronized (allTaskStatistics)
      {
        com.evolving.nglm.core.SinkTaskStatistics taskStatistics = allTaskStatistics.get(statisticsKey(connectorName, taskNumber));
        if (taskStatistics == null)
          {
            try
              {
                taskStatistics = new com.evolving.nglm.core.SinkTaskStatistics(connectorName, "Elasticsearch", taskNumber);
              }
            catch (com.evolving.nglm.core.ServerException se)
              {
                throw new ServerRuntimeException("Could not create statistics object", se);
              }
            allTaskStatistics.put(statisticsKey(connectorName, taskNumber), taskStatistics);
          }
        return taskStatistics;
      }
  }    

  /*****************************************
  *
  *  updateBatchCount
  *
  *****************************************/

  private void updateBatchCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        com.evolving.nglm.core.SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateBatchCount(amount);
      }
  }

  /*****************************************
  *
  *  updatePutCount
  *
  *****************************************/

  private void updatePutCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        com.evolving.nglm.core.SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updatePutCount(amount);
      }
  }

  /*****************************************
  *
  *  updateRecordCount
  *
  *****************************************/

  private void updateRecordCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        com.evolving.nglm.core.SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateRecordCount(amount);
      }
  }

  /*****************************************
  *
  *  stopStatisticsCollection
  *
  *****************************************/

  private void stopStatisticsCollection()
  {
    synchronized (allTaskStatistics)
      {
        for (com.evolving.nglm.core.SinkTaskStatistics taskStatistics : allTaskStatistics.values())
          {
            taskStatistics.unregister();
          }
        allTaskStatistics.clear();
      }
  }
}