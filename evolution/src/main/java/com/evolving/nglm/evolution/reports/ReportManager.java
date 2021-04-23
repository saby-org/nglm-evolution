/****************************************************************************
 *
 *  ReportManager.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.DynamicCriterionFieldService;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

/**
 * This class uses Zookeeper to launch the generation of reports. 
 * When a node is created in the controlDir, it triggers the generation of a report.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report (of the same type) to be created. 
 *
 */
public class ReportManager implements Watcher
{
  private static final Logger log = LoggerFactory.getLogger(ReportManager.class);
  private static final int MINUTES_BETWEEN_MEMORY_LOGS = 60;
  protected static final String CONTROL_SUBDIR = "control"; // used in ReportScheduler
  protected static final String LOCK_SUBDIR = "lock";
  protected static final int sessionTimeout = 30*1000; // 30 seconds
  
  protected ZooKeeper zk = null;
  protected static String zkHostList;
  protected static String brokerServers;
  protected static String esNode;
  protected DateFormat dfrm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z z");   // TODO EVPRO-99
  private static ReportManagerStatistics reportManagerStatistics;
  private ReportService reportService = null;
  private boolean gotSessionExpired;
  private boolean gotFatalError;
  private String controlFileToRemove = null;
  
  public static short replicationFactor;
  public static int nbPartitions;
  public static int standbyReplicas;

  private static String controlDir;
  private static String lockDir;
  private static String topDir;

  //this will be overwriten in inherited classes. IN this way the inherited classes will not have to implement process methos from watcher
  protected String serviceControlDir;

  static
    {
      topDir = Deployment.getReportManagerZookeeperDir();
      controlDir = topDir + File.separator + CONTROL_SUBDIR;
      lockDir = topDir + File.separator + LOCK_SUBDIR;
    }

  /**
   * Used by ReportScheduler to launch reports.
   */
  public static String getControlDir() {
    return controlDir;
  }

  /**
   * Top zookeeper hierarchy.
   */
  public static String getTopDir()
  {
    return topDir;
  }

  /**
   * Used to check if report is running
   */
  public String getLockDir()
  {
    return lockDir;
  }

  /*****************************************
   *
   *  constructor
   *
   *****************************************/
  public ReportManager()
  {
    DynamicCriterionFieldService dynamicCriterionFieldService = new DynamicCriterionFieldService(brokerServers, "reportmanager-dynamiccriterionfieldservice-001", Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);
  }

  /*****************************************
  *
  *  initializeReportManager
  *  this part were defined in default constructor. Because ExtractManager extend this class default contructor is called so the code were moved in method
  *
  *****************************************/
  
  private void initializeReportManager() throws Exception
  {
    gotSessionExpired = false;
    gotFatalError = false;
    log.debug("controlDir = "+controlDir+" , lockDir = "+lockDir);

    serviceControlDir = controlDir;

    ReportListener reportListener = new ReportListener() {
      @Override public void reportActivated(Report report) {
        log.trace("report activated : " + report);
      }
      @Override public void reportDeactivated(String guiManagedObjectID) {
        log.trace("report deactivated: " + guiManagedObjectID);
      }
    };

    if (reportService == null)
      {
        // first time
        log.trace("Creating ReportService");
        reportService = new ReportService(Deployment.getBrokerServers(), "reportmanager-reportservice-001", Deployment.getReportTopic(), false, reportListener);
        reportService.start();
        log.trace("ReportService started");
      }

    zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
    log.debug("ZK client created : "+zk);
    // TODO next 3 lines could be done once for all in nglm-evolution/.../evolution-setup-zookeeper.sh
    createZKNode(topDir, true);
    createZKNode(controlDir, true);
    createZKNode(lockDir, true);
    
    if (controlFileToRemove != null)
      {
        while (true)
          {
            try 
            {
              log.info("Deleting remaining control file from last execution "+controlFileToRemove);
              zk.delete(controlFileToRemove, -1);
            }
            catch (KeeperException e) {
              if (e.code() != Code.NONODE)
                {
                  handleSessionExpired(e, "Issue deleting control file from last execution " + controlFileToRemove);
                  // if we get a KeeperException like ConnectionLossException, keep trying to delete file, until ZK is back, or the session expires
                  Thread.sleep(5000);
                  continue;
                }
            }
            catch (InterruptedException e)
            {
              log.info("Interrupted deleting control file from last execution " + controlFileToRemove + " : "+e.getLocalizedMessage(), e);
              continue;
            }
            break;
          }
      }

    while (true)
      {
        try
        {
          List<String> initialReportList = zk.getChildren(serviceControlDir, null); // no watch initially
          processChildren(initialReportList);
        }
        catch (KeeperException e) {
          handleSessionExpired(e, "Issue in initial getChildren 1, retry " + e.getLocalizedMessage());
          Thread.sleep(5000);
          continue;
        }
        catch (InterruptedException e) 
        {
          log.info("Interrupted in initial getChildren 1, retry "+e.getLocalizedMessage(), e);
          continue;
        }
        break;
      }
          
    while (true)
      {
        try
        {
          zk.getChildren(serviceControlDir, this); // only to set watch
        }
        catch (KeeperException e) {
          handleSessionExpired(e, "Issue in initial getChildren 2, retry " + e.getLocalizedMessage());
          Thread.sleep(5000);
          continue;
        }
        catch (InterruptedException e) 
        {
          log.info("Interrupted in initial getChildren 2, retry "+e.getLocalizedMessage(), e);
          continue;
        }
        break;
      }
  }

  /*****************************************
  *
  *  createZKNode
  *
  *****************************************/
  
  protected void createZKNode(String znode, boolean canExist) {
    while (true)
      {
        log.info("Trying to create znode "	+ znode + " (" + (canExist?"may":"must not")+" already exist)");
        try
        {
          zk.create(znode, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } 
        catch (KeeperException e)
        {
          if (canExist && (e.code() == KeeperException.Code.NODEEXISTS)) 
            {
              log.trace(znode+" already exists, this is OK");
            }
          else 
            {
              handleSessionExpired(e, "Error creating node " + znode);
              // We don't care about SessionExpired, because we just connected, and we would not know what to do in this case (we're already restarting)  
              if (e.code() == KeeperException.Code.CONNECTIONLOSS)
                {
                  try { Thread.sleep(5000); } catch (InterruptedException ie) {}
                  continue;
                }
            }
        }
        catch (InterruptedException e)
        {
          log.info("Got " + e.getLocalizedMessage(), e);
        }
        break;
      }
  }

  @Override
  public void process(WatchedEvent event) 
  {
    log.trace("Got event : "+event);
    if (event.getType().equals(EventType.NodeChildrenChanged)) 
      {
        try 
        {
          List<String> children = zk.getChildren(serviceControlDir, this); // get the children and renew watch
          processChildren(children);
        }
        catch (KeeperException e) { handleSessionExpired(e, "Error processing report"); }
        catch (InterruptedException e)
        {
          log.error("Error processing report", e);
        }
        catch (Error e) // in case of fatal error, such as OutOfMemory, make sure the container exits
        {
          gotFatalError = true;
          log.error("Fatal error processing report", e);
        }
      }
  }

  /*****************************************
   *
   * processChildren
   *
   *****************************************/

  protected void processChildren(List<String> children) throws InterruptedException
  {
    if (children != null && !children.isEmpty())
      {
        Collections.sort(children); // we are getting an unsorted list
        for (String child : children)
          {
            processChild(child);
          }
      }
  }
  protected void processChild(String child) throws InterruptedException
  {
    // Generate report in separate thread
    Thread thread = new Thread( () -> { 
      try
      {
        processChild2(child);
      }
      catch (InterruptedException e)
      {
        log.trace("Failed to process control file " +child+ ":"+e.getLocalizedMessage(), e);
      }
    } );
    thread.start();
    
    // Wait some random time (10-60 sec), so that when ReportManager starts with a big backlog, all threads do not start simultaneously
    long waitTimeSec = 10L + (long) (new java.util.Random().nextInt(50));
    log.info("Wait " + waitTimeSec + " seconds for contention management...");
    try { Thread.sleep(waitTimeSec*1000L); } catch (InterruptedException ie) {}
    log.trace("Finished Wait " + waitTimeSec + " seconds");
  }

  // Called when a control file is created or deleted
  public void processChild2(String child) throws InterruptedException
  {
    String controlFile = controlDir + File.separator + child;
    String lockFile = lockDir + File.separator + child;
    log.trace("Checking if control exists : "+controlFile); // We might have been called for the suppression of the control node
    try
    {
      if (zk.exists(controlFile, false) != null)
        {
          log.trace("Checking if lock exists : "+lockFile);
          if (zk.exists(lockFile, false) == null) 
            {
              log.trace("Processing entry "+child+" with znodes "+controlFile+" and "+lockFile);
              try
              {
                log.trace("Trying to create lock "+lockFile);
                zk.create(lockFile, dfrm.format(SystemTime.getCurrentTime()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                try 
                {
                  log.trace("Lock "+lockFile+" successfully created");
                  Stat stat = null;
                  Charset utf8Charset = Charset.forName("UTF-8");
                  byte[] d = zk.getData(controlFile, false, stat);
                  String data = new String(d, utf8Charset);
                  log.info("Got data "+data);
                  Scanner s = new Scanner(data + "\n");
                  String reportMetadata = s.next().trim();
                  JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(reportMetadata);
                  String reportName = JSONUtilities.decodeString(jsonRoot, "reportName", true);
                  final Date reportGenerationDate = new Date(JSONUtilities.decodeLong(jsonRoot, "reportGenerationDate", true));
                  int tenantID = JSONUtilities.decodeInteger(jsonRoot, "tenantID", true);
                  String restOfLine = s.nextLine().trim();
                  s.close();
                  Collection<GUIManagedObject> reports = reportService.getStoredReports(tenantID);
                  Report report = null;
                  if (reportName != null)
                    {
                      for (GUIManagedObject gmo : reports)
                        {
                          if (gmo instanceof Report)
                            {
                              Report reportLocal = (Report) gmo;
                              log.trace("Checking "+reportLocal+" for "+reportName);
                              if (reportName.equals(reportLocal.getName())) 
                                {
                                  report = reportLocal;
                                  break;
                                }
                            }
                        }
                    }
                  if (report == null)
                    {
                      log.error("Report does not exist : "+reportName);
                      reportManagerStatistics.incrementFailureCount();
                    } 
                  else
                    {
                      boolean allOK = false;
                      int safeguardCount = 0;
                      while (!allOK)
                        {
                          log.debug("report = "+report);
                          log.info("JVM free memory : {} over total of {}", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()), FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
                          allOK = handleReport(reportName, reportGenerationDate, report, restOfLine, tenantID);
                          reportManagerStatistics.incrementReportCount();
                          if (!allOK)
                            {
                              if (safeguardCount > 3)
                                {
                                  log.info("There was an issue producing " + reportName + " for tenant " + tenantID + ", stop retrying");
                                  allOK = true; // after a while, stop, this should stay exceptional
                                }
                              else
                                {
                                  log.info("There was an issue producing " + reportName + " for tenant " + tenantID + ", restarting it after 1 minute, occurence " + (++safeguardCount));
                                  try { Thread.sleep(60*1000L); } catch (InterruptedException ie) {} // wait 1 minute
                                }
                            }
                        }
                      long beforeGC = System.currentTimeMillis();
                      long freeMemoryBefore = Runtime.getRuntime().freeMemory();
                      long allMemoryBefore = Runtime.getRuntime().totalMemory();
                      log.info("Before GC JVM free memory : {} over total of {}",
                          FileUtils.byteCountToDisplaySize(freeMemoryBefore), FileUtils.byteCountToDisplaySize(allMemoryBefore));
                      System.gc();
                      System.runFinalization();
                      long afterGC = System.currentTimeMillis();
                      long freeMemoryAfter = Runtime.getRuntime().freeMemory();
                      long allMemoryAfter = Runtime.getRuntime().totalMemory();
                      log.info("After GC JVM free memory : {} over total of {}, {} reclaimed, took {} ms",
                          FileUtils.byteCountToDisplaySize(freeMemoryAfter), FileUtils.byteCountToDisplaySize(allMemoryAfter),
                          FileUtils.byteCountToDisplaySize(freeMemoryAfter-freeMemoryBefore), (afterGC-beforeGC));
                    }
                }
                catch (KeeperException e) { handleSessionExpired(e, "Issue while reading from control node "+controlFile); }
                catch (InterruptedException | NoSuchElementException e)
                {
                  log.error("Issue while reading from control node "+e.getLocalizedMessage(), e);
                  reportManagerStatistics.incrementFailureCount();
                }
                catch (IllegalCharsetNameException e)
                {
                  log.error("Unexpected issue, UTF-8 does not seem to exist "+e.getLocalizedMessage(), e);
                  reportManagerStatistics.incrementFailureCount();
                }
                catch (Exception e) // this is OK because we trace the root cause, and we'll fix it
                {
                  log.error("Unexpected issue " + e.getLocalizedMessage(), e);
                  reportManagerStatistics.incrementFailureCount();
                }

                // Delete control file
                while (true)
                  {
                    log.info("Deleting control "+controlFile);
                    try
                    {
                      zk.delete(controlFile, -1);
                    }
                    catch (KeeperException e) { 
                      handleSessionExpired(e, "Issue deleting control "+controlFile);
                      if (e.code() == Code.SESSIONEXPIRED)
                        {
                          // We need to remove the control file, so that the same report is not restarted when everything restarts
                          controlFileToRemove = controlFile;
                        }
                      else if (e.code() != Code.NONODE)
                        {
                          // if we get a KeeperException like ConnectionLossException, keep trying to delete file, until ZK is back, or the session expires
                          Thread.sleep(5000);
                          continue;
                        }
                    }
                    catch (InterruptedException e) 
                    {
                      log.info("Interrupted deleting control : "+e.getLocalizedMessage(), e);
                      continue;
                    }
                    break;
                  }

                // Delete lock file
                while (true)
                  {
                    try 
                    {
                      log.info("Deleting lock "+lockFile);
                      zk.delete(lockFile, -1);
                    }
                    catch (KeeperException e) {
                      handleSessionExpired(e, "Issue deleting lock " + lockFile);
                      if (e.code() != Code.SESSIONEXPIRED && e.code() != Code.NONODE)
                        {
                          // if we get a KeeperException like ConnectionLossException, keep trying to delete file, until ZK is back, or the session expires
                          Thread.sleep(5000);
                          continue;
                        }
                    }
                    catch (InterruptedException e)
                    {
                      log.info("Interrupted deleting lock : "+e.getLocalizedMessage(), e);
                      continue;
                    }
                    break;
                  }
                log.info("Both files deleted");
              }
              catch (KeeperException e) { handleSessionExpired(e, "Failed to create lockfile, this is OK " + lockFile); }
              catch (InterruptedException ignore)
              {
                // even so we check the existence of a lock, it could have been created in the mean time making create fail. We catch and ignore it.
                log.trace("Failed to create lockfile, this is OK " +lockFile+ ":"+ignore.getLocalizedMessage(), ignore);
              } 
            } 
          else 
            {
              log.trace("--> This report is already processed, skip it");
            }
        }
    }
    catch (KeeperException e) { handleSessionExpired(e, "Issue while reading from lock or control file " + lockFile + " " + controlFile); }
  }

  private void handleSessionExpired(KeeperException e, String msg)
  {
    if (e.code() == Code.SESSIONEXPIRED || e.code() == Code.CONNECTIONLOSS)
      {
        gotSessionExpired = true;
      }
    log.error(msg, e);
    reportManagerStatistics.incrementFailureCount();
  }

  /*****************************************
  *
  *  handleReport
  *
  *****************************************/
  
  private boolean handleReport(String reportName, final Date reportGenerationDate, Report report, String restOfLine, int tenantID)
  {
    long startReport = System.currentTimeMillis();
    log.trace("---> Starting report " + reportName + " " + restOfLine);
    boolean allOK = true;
    String[] params = null;
    if (!"".equals(restOfLine))
      {
        params = restOfLine.split("\\s+"); // split with spaces
      }
    if (params != null)
      {
        for (String param : params)
          {
            log.debug("  param : " + param);
          }
      }
    try
      {
        String outputPath = ReportService.getReportOutputPath(tenantID);
        log.trace("outputPath = " + outputPath);
        String dateFormat = Deployment.getDeployment(tenantID).getReportManagerDateFormat();
        log.trace("dateFormat = " + dateFormat);
        String fileExtension = Deployment.getDeployment(tenantID).getReportManagerFileExtension();
        log.trace("fileExtension = " + fileExtension);
        SimpleDateFormat sdf;
        try
          {
            sdf = new SimpleDateFormat(dateFormat);    // TODO EVPRO-99
          } 
        catch (IllegalArgumentException e)
          {
            log.error("Config error : date format " + dateFormat + " is invalid, using default" + e.getLocalizedMessage(), e);
            sdf = new SimpleDateFormat(); // Default format, might not be valid in a filename, sigh...    // TODO EVPRO-99
          }
        sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
        String fileSuffix = sdf.format(reportGenerationDate);
        String csvFilename = "" + outputPath + File.separator + reportName + "_" + tenantID + "_" + fileSuffix + "." + fileExtension;
        log.trace("csvFilename = " + csvFilename);

        @SuppressWarnings("unchecked")
        Class<ReportDriver> reportClass = (Class<ReportDriver>) Class.forName(report.getReportClass());
        Constructor<ReportDriver> cons = reportClass.getConstructor();
        ReportDriver rd = cons.newInstance((Object[]) null);
        try
          {
            rd.produceReport(report, reportGenerationDate, zkHostList, brokerServers, esNode, csvFilename, params, tenantID);
          } 
        catch (Exception e)
          {
            // handle any kind of exception that can happen during generating the report, and do not crash the container
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.error("Exception processing report " + reportName + " in " + csvFilename + " : {}", stackTraceWriter.toString().replace('\n', '\u2028'));
            // the report may have been partially generated. We need to remove it and restart
            Path path = FileSystems.getDefault().getPath(csvFilename + ReportUtils.ZIP_EXTENSION);
            try
              {
                log.info("Removing partial report " + csvFilename + ReportUtils.ZIP_EXTENSION);
                Files.delete(path);
                allOK = false; // need to restart this report
              }
            catch (IOException e1)
              {
                if (e1 instanceof java.nio.file.NoSuchFileException)
                  {
                    log.debug("Report file was not created yet");
                    allOK = false; // need to restart this report
                  }
                else
                  {
                    log.info("issue when deleting partial report " + csvFilename + " : " + e1.getClass().getCanonicalName()+ " " + e1.getLocalizedMessage());
                    // if unable to delete report, do not restart it (allOK = true)
                  }
              }
          }
        log.trace("---> Finished report " + reportName + " in " + csvFilename);
      } 
    catch (ClassNotFoundException e)
      {
        log.error("Undefined class name " + e.getLocalizedMessage(), e);
        reportManagerStatistics.incrementFailureCount();
      } 
    catch (NoSuchMethodException e)
      {
        log.error("Undefined method " + e.getLocalizedMessage(), e);
        reportManagerStatistics.incrementFailureCount();
      } 
    catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
      {
        log.error("Error : " + e.getLocalizedMessage(), e);
        reportManagerStatistics.incrementFailureCount();
      }
    log.info("Finished report " + reportName + " took " + (System.currentTimeMillis() - startReport) + " ms");
    return allOK;
  }

  /*****************************************
   *
   * main
   *
   *****************************************/

  public static void main(String[] args)
  {
    new LoggerInitialization().initLogger();
    log.info("ReportManager: received " + args.length + " args");
    for (String arg : args)
      {
        log.info("ReportManager main : arg " + arg);
      }
    if (args.length < 5)
      {
        log.error("Usage : ReportManager BrokerServers ESNode replication partitions standby");
        System.exit(1);
      }
    brokerServers = args[0];
    esNode = args[1];
    replicationFactor = Short.parseShort(args[2]);
    nbPartitions = Integer.parseInt(args[3]);
    standbyReplicas = Integer.parseInt(args[4]);

    zkHostList = Deployment.getZookeeperConnect();

    reportManagerStatistics = new ReportManagerStatistics("reportmanager");
    ReportManager rm = new ReportManager();
    int minuteCounter;
    try 
    {
      while (true) // we loop to handle session expiration
        {
          minuteCounter = MINUTES_BETWEEN_MEMORY_LOGS;
          rm.initializeReportManager();
          while (true)
            { //  sleep forever
              try 
              {
                if (rm.gotFatalError)
                  {
                    return; // to exit JVM, and restart docker container
                  }
                if (rm.gotSessionExpired)
                  {
                    // got exception, should reconnect to ZK
                    rm.gotSessionExpired = false;
                    break;
                  }
                Thread.sleep(60*1000); // check every minute
                if (minuteCounter-- < 0) // display memory size every hour
                  {
                    minuteCounter = MINUTES_BETWEEN_MEMORY_LOGS;
                    log.info("JVM free memory : {} over total of {}", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()), FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
                  }
              } catch (InterruptedException ignore) {}
            }
        }
    }
    catch (Exception e)
    {
      log.error("Exception in ReportManager, will stop processing reports, this must be fixed : "+e.getLocalizedMessage(), e);
    }
  }

}
