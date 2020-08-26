/****************************************************************************
 *
 *  ReportManager.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.evolving.nglm.evolution.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ReportService.ReportListener;

/**
 * This class uses Zookeeper to launch the generation of reports. 
 * When a node is created in the controlDir, it triggers the generation of a report.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report (of the same type) to be created. 
 *
 */
public class ReportManager implements Watcher
{

  protected static final String CONTROL_SUBDIR = "control"; // used in ReportScheduler
  protected static final String LOCK_SUBDIR = "lock";
  protected static final int sessionTimeout = 10*1000; // 60 seconds
  protected ZooKeeper zk = null;
  protected static String zkHostList;
  protected static String brokerServers;
  protected static String esNode;
  protected DateFormat dfrm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z z");
  private static final Logger log = LoggerFactory.getLogger(ReportManager.class);
  private static ReportManagerStatistics reportManagerStatistics;
  private ReportService reportService = null;
  private boolean gotSessionExpired;
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

    gotSessionExpired = false;
    zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
    log.debug("ZK client created : "+zk);
    // TODO next 3 lines could be done once for all in nglm-evolution/.../evolution-setup-zookeeper.sh
    createZKNode(topDir, true);
    createZKNode(controlDir, true);
    createZKNode(lockDir, true);
    
    if (controlFileToRemove != null)
      {
        log.info("Deleting remaining control file from last execution "+controlFileToRemove);
        try
        {
          zk.delete(controlFileToRemove, -1);
        }
        catch (KeeperException e) { 
          handleSessionExpired(e, "Issue deleting control "+controlFileToRemove);
        }
      }

    List<String> initialReportList = zk.getChildren(serviceControlDir, null); // no watch initially
    try
    {
      processChildren(initialReportList);
    } catch (InterruptedException e)
    {
      log.error("Error processing report", e);
    }
    zk.getChildren(serviceControlDir, this); // sets watch
  }

  /*****************************************
  *
  *  createZKNode
  *
  *****************************************/
  
  protected void createZKNode(String znode, boolean canExist) {
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
        }
    }
    catch (InterruptedException e)
    {
      log.info("Got "+e.getLocalizedMessage(), e);
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
      }
  }
  
  /*****************************************
  *
  *  processChildren
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
  }

  public void processChild2(String child) throws InterruptedException
  {
    String controlFile = controlDir + File.separator + child;
    String lockFile = lockDir + File.separator + child;
    log.trace("Checking if lock exists : "+lockFile);
    try
    {
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
              Scanner s = new Scanner(data+"\n"); // Make sure s.nextLine() will work
              String reportName = s.next().trim();
              log.trace("We got reportName = "+reportName);
              String restOfLine = s.nextLine().trim();
              s.close();
              Collection<GUIManagedObject> reports = reportService.getStoredReports();
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
                  log.debug("report = "+report);
                  handleReport(reportName, report, restOfLine);
                  reportManagerStatistics.incrementReportCount();
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
            finally 
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
              }
              catch (InterruptedException e) 
              {
                log.info("Issue deleting control : "+e.getLocalizedMessage(), e);
              }
              finally 
              {
                log.info("Deleting lock "+lockFile);
                try 
                {
                  zk.delete(lockFile, -1);
                  log.info("Both files deleted");
                }
                catch (KeeperException e) { handleSessionExpired(e, "Issue deleting lock " + lockFile); }
                catch (InterruptedException e)
                {
                  log.info("Issue deleting lock : "+e.getLocalizedMessage(), e);
                }
              }
            }
          }
          catch (KeeperException e) { handleSessionExpired(e, "Failed to create lock file, this is OK " + lockFile); }
          catch (InterruptedException ignore)
          {
            // even so we check the existence of a lock, it could have been created in the mean time making create fail. We catch and ignore it.
            log.trace("Failed to create lock file, this is OK " +lockFile+ ":"+ignore.getLocalizedMessage(), ignore);
          } 
        } 
      else 
        {
          log.trace("--> This report is already processed by another ReportManager instance");
        }
    }
    catch (KeeperException e) { handleSessionExpired(e, "Issue while reading from lock " + lockFile); }
  }

  private void handleSessionExpired(KeeperException e, String msg)
  {
    if (e.code() == Code.SESSIONEXPIRED)
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
  
  private void handleReport(String reportName, Report report, String restOfLine)
  {
    log.trace("---> Starting report "+reportName+" "+restOfLine);
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
      String outputPath = Deployment.getReportManagerOutputPath();
      log.trace("outputPath = "+outputPath);
      String dateFormat = Deployment.getReportManagerDateFormat();
      log.trace("dateFormat = "+dateFormat);
      String fileExtension = Deployment.getReportManagerFileExtension();
      log.trace("dateFormat = "+fileExtension);

      SimpleDateFormat sdf;
      try {
        sdf = new SimpleDateFormat(dateFormat);
      } catch (IllegalArgumentException e) {
        log.error("Config error : date format "+dateFormat+" is invalid, using default"+e.getLocalizedMessage(), e);
        sdf = new SimpleDateFormat(); // Default format, might not be valid in a filename, sigh...
      }
      sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      String fileSuffix = sdf.format(SystemTime.getCurrentTime());
      String csvFilename = "" 
          + outputPath 
          + File.separator
          + reportName
          + "_"
          + fileSuffix
          + "."
          + fileExtension;
      log.trace("csvFilename = " + csvFilename);

      @SuppressWarnings("unchecked")
      Class<ReportDriver> reportClass = (Class<ReportDriver>) Class.forName(report.getReportClass());
      Constructor<ReportDriver> cons = reportClass.getConstructor();
      ReportDriver rd = cons.newInstance((Object[]) null);
      try
      {
        rd.produceReport(report, zkHostList, brokerServers, esNode, csvFilename, params);
      }
      catch (Exception e)
      {
        // handle any kind of exception that can happen during generating the report, and do not crash the container
        log.error("Exception processing report " + reportName + " : " + e);
      }
      log.trace("---> Finished report "+reportName);
    }
    catch (ClassNotFoundException e)
    {
      log.error("Undefined class name "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    }
    catch (NoSuchMethodException e)
    {
      log.error("Undefined method "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    }
    catch (SecurityException|InstantiationException|IllegalAccessException|
        IllegalArgumentException|InvocationTargetException e) 
    {
      log.error("Error : "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    }
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) 
  {
    new LoggerInitialization().initLogger();
    log.info("ReportManager: received " + args.length + " args");
    for(String arg : args)
      {
        log.info("ReportManager main : arg " + arg);
      }
    if (args.length < 5) 
      {
        log.error("Usage : ReportManager BrokerServers ESNode replication partitions standby");
        System.exit(1);
      }
    brokerServers = args[0];
    esNode        = args[1];
    replicationFactor = Short.parseShort(args[2]);
    nbPartitions = Integer.parseInt(args[3]);
    standbyReplicas = Integer.parseInt(args[4]);
    
    zkHostList = Deployment.getZookeeperConnect();

    try 
    {
      reportManagerStatistics = new ReportManagerStatistics("reportmanager");
      ReportManager rm = new ReportManager();
      while (true) // we loop to handle session expiration
        {
          rm.initializeReportManager();
          while (true) 
            { //  sleep forever
              try 
              {
                if (rm.gotSessionExpired)
                  {
                    // got exception, should reconnect to ZK
                    rm.gotSessionExpired = false;
                    break;
                  }
                Thread.sleep(60*1000); // check every minute
              } catch (InterruptedException ignore) {}
            }
        }
    }
    catch (Exception e)
    {
      log.info("Issue in Zookeeper : "+e.getLocalizedMessage(), e);
    }
  }

}
