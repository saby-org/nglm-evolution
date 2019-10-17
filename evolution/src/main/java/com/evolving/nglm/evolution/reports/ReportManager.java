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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

/**
 * This class uses Zookeeper to launch the generation of reports. 
 * When a node is created in the controlDir, it triggers the generation of a report.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report
 * (of the same type) to be created. 
 *
 */
public class ReportManager implements Watcher{

  public static final String CONTROL_SUBDIR = "control"; // used in ReportScheduler
  private static final String LOCK_SUBDIR = "lock";
  private static final int sessionTimeout = 10*1000; // 60 seconds

  private static String controlDir = null;
  private String lockDir = null;
  private ZooKeeper zk = null;
  private static String zkHostList;
  private static String brokerServers;
  private static String esNode;
  private DateFormat dfrm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z z");
  private static final Logger log = LoggerFactory.getLogger(ReportManager.class);
  private static ReportManagerStatistics reportManagerStatistics;
  private ReportService reportService;

  /**
   * Used by ReportScheduler to launch reports.
   */
  public static String getControlDir() {
    return getTopDir() + File.separator + CONTROL_SUBDIR;
  }

  /**
   * Top zookeeper hierarchy.
   */
  public static String getTopDir() {
    return Deployment.getReportManagerZookeeperDir();
  }
  
  /**
   * Used to check if report is running
   */
  public static String getLockDir() {
    return getTopDir() + File.separator + LOCK_SUBDIR;
  }

  public ReportManager() throws Exception {
    String topDir = getTopDir();
    controlDir = getControlDir();
    lockDir = topDir + File.separator + LOCK_SUBDIR;
    log.debug("controlDir = "+controlDir+" , lockDir = "+lockDir);
    
    ReportListener reportListener = new ReportListener() {
      @Override public void reportActivated(Report report) {
        log.trace("report activated : " + report);
      }
      @Override public void reportDeactivated(String guiManagedObjectID) {
        log.trace("report deactivated: " + guiManagedObjectID);
      }
    };
    log.trace("Creating ReportService");
    reportService = new ReportService(
        Deployment.getBrokerServers(),
        "reportmanagerer-reportservice-001",
        Deployment.getReportTopic(),
        false,
        reportListener);
    reportService.start();
    log.trace("ReportService started");

    zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
    log.debug("ZK client created : "+zk);
    // TODO next 3 lines could be done once for all in nglm-evolution/.../evolution-setup-zookeeper.sh
    createZKNode(topDir, true);
    createZKNode(controlDir, true);
    createZKNode(lockDir, true);
    List<String> children = zk.getChildren(controlDir, this); // sets watch
  }

  private void createZKNode(String znode, boolean canExist) {
    log.debug("Trying to create znode "	+ znode
        + " (" + (canExist?"may":"must not")+" already exist)");
    try {
      zk.create(znode, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException e) {
      if (canExist && (e.code() == KeeperException.Code.NODEEXISTS)) {
        log.trace(znode+" already exists, this is OK");
      } else {
        log.info("Got "+e.getLocalizedMessage());
      }
    } catch (InterruptedException e) {
      log.info("Got "+e.getLocalizedMessage(), e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    log.trace("Got event : "+event);
    try {
      if (event.getType().equals(EventType.NodeChildrenChanged)) {
        List<String> children = zk.getChildren(controlDir, this); // get the children and renew watch
        if (!children.isEmpty()) {
          Collections.sort(children); // we are getting an unsorted list
          for (String child : children) {
            String controlFile = controlDir + File.separator + child;
            String lockFile = lockDir + File.separator + child;
            log.trace("Checking if lock file exists : "+lockFile);
            if (zk.exists(lockFile, false) == null) {
              log.trace("Processing entry "+child+" with znodes "+controlFile+" and "+lockFile);
              try {
                log.trace("Trying to create lock file "+lockFile);
                zk.create(
                    lockFile,
                    dfrm.format(SystemTime.getCurrentTime()).getBytes(), 
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
                try {
                  log.trace("Lock file "+lockFile+" successfully created");
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
                  if (reportName != null) {
                    for (GUIManagedObject gmo : reports) {
                      Report reportLocal = (Report) gmo;
                      log.trace("Checking "+report+" for "+reportName);
                      if (reportName.equals(reportLocal.getName())) {
                        report = reportLocal;
                        break;
                      }
                    }
                  }
                  if (report == null) {
                    log.error("Report does not exist : "+reportName);
                    reportManagerStatistics.incrementFailureCount();
                  } else {
                    log.debug("report = "+report);
                    handleReport(reportName, report, restOfLine);
                    reportManagerStatistics.incrementReportCount();
                  }
                } catch (KeeperException | InterruptedException | NoSuchElementException e) {
                  log.error("Issue while reading from control node "+e.getLocalizedMessage(), e);
                  reportManagerStatistics.incrementFailureCount();
                } catch (IllegalCharsetNameException e) {
                  log.error("Unexpected issue, UTF-8 does not seem to exist "+e.getLocalizedMessage(), e);
                  reportManagerStatistics.incrementFailureCount();
                } finally {
                  try {
                    log.trace("Deleting control file "+controlFile);
                    zk.delete(controlFile, -1);
                  } catch (KeeperException | InterruptedException e) {
                    log.trace("Issue deleting control file : "+e.getLocalizedMessage(), e);
                  }
                  try {
                    log.trace("Deleting lock file "+lockFile);
                    zk.delete(lockFile, -1);
                    log.trace("Both files deleted");
                  } catch (KeeperException | InterruptedException e) {
                    log.trace("Issue deleting lock file : "+e.getLocalizedMessage(), e);
                  }
                }
              } catch (KeeperException | InterruptedException ignore) {
                // even so we check the existence of a lock,
                // it could have been created in the mean time
                // making create fail. We catch and ignore it.
                log.trace("Failed to create lock file, this is OK "
                    +lockFile+ ":"+ignore.getLocalizedMessage(), ignore);
              } 
            } else {
              log.trace("--> This report is already processed by another ReportManager instance");
            }
          }
        }
      }
    } catch (Exception e){
      log.error("Error processing report", e);
    }
  }

  private void handleReport(String reportName, Report report, String restOfLine) {
    log.trace("Processing "+reportName+" "+restOfLine);
    String[] params = null;
    if (!"".equals(restOfLine)) {
      params = restOfLine.split("\\s+"); // split with spaces
    }
    if (params != null) {
      for (String param : params) {
        log.debug("  param : " + param);
      }
    }
    try {
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
      rd.produceReport(report, zkHostList, brokerServers, esNode, csvFilename, params);
    } catch (ClassNotFoundException e) {
      log.error("Undefined class name "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    } catch (NoSuchMethodException e) {
      log.error("Undefined method "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    } catch (SecurityException|InstantiationException|IllegalAccessException|
        IllegalArgumentException|InvocationTargetException e) {
      log.error("Error : "+e.getLocalizedMessage(), e);
      reportManagerStatistics.incrementFailureCount();
    }
  }

  public static void main(String[] args) {
    log.info("ReportManager: received " + args.length + " args");
    for(String arg : args){
      log.info("ReportManager main : arg " + arg);
    }
    if (args.length < 2) {
      log.error("Usage : ReportManager BrokerServers ESNode");
      System.exit(1);
    }
    brokerServers = args[0];
    esNode        = args[1];
    zkHostList = Deployment.getZookeeperConnect();
    try {
      reportManagerStatistics = new ReportManagerStatistics("reportmanager");
      ReportManager rm = new ReportManager();
      log.debug("ZK client created");
      while (true) { //  sleep forever
        try {
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ignore) {}
      }
    } catch (Exception e) {
      log.info("Issue in Zookeeper : "+e.getLocalizedMessage(), e);
    }
  }

}
