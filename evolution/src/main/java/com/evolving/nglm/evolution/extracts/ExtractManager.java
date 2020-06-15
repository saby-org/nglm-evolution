/****************************************************************************
 *
 *  ReportManager.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.reports.ReportManager;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class uses Zookeeper to launch the generation of reports. 
 * When a node is created in the controlDir, it triggers the generation of a report.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report (of the same type) to be created. 
 *
 */
public class ExtractManager extends ReportManager
{

  //public static final String CONTROL_SUBDIR = "control"; // used in ReportScheduler
  //private static final String LOCK_SUBDIR = "lock";
  //private static final int sessionTimeout = 10*1000; // 60 seconds
  //
  //private static String controlDir = null;
  //private String lockDir = null;
  //private ZooKeeper zk = null;
  //private static String zkHostList;
  //private static String brokerServers;
  //private static String esNode;
  //private DateFormat dfrm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z z");
  private static final Logger log = LoggerFactory.getLogger(ExtractManager.class);
  private static ExtractManagerStatistics extractManagerStatistics;
  //private TargetService targetService;
  //
  //public static short replicationFactor;
  //public static int nbPartitions;
  //public static int standbyReplicas;

  //because control dir is used in gui manager this have to be defined as private and cannot set value from base class
  private static String controlDir;
  private static String lockDir;
  private static String topDir;

  /**
   * Used to launch extracts.
   */

  static
    {
      topDir = Deployment.getExtractManagerZookeeperDir();
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
  
  public ExtractManager() throws Exception
  {
    log.debug("controlDir = "+controlDir+" , lockDir = "+lockDir);

    //override the serviceCOntrol dir used in watcher.
    // The static value cannot be used please read comment for control dir variable defined above
    //this is done to not replicate the implementation of process method from watcher with the same code in this class
    serviceControlDir = controlDir;

    zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
    log.debug("ZK client created : "+zk);
    // TODO next 3 lines could be done once for all in nglm-evolution/.../evolution-setup-zookeeper.sh
    createZKNode(topDir, true);
    createZKNode(controlDir, true);
    createZKNode(lockDir, true);
    List<String> initialTargetList = zk.getChildren(controlDir, null); // no watch initially
    try
    {
      processChildren(initialTargetList);
    } catch (KeeperException | InterruptedException e)
    {
      log.error("Error processing extract", e);
    }
    zk.getChildren(serviceControlDir, this); // sets watch
  }

  ///*****************************************
  //*
  //*  createZKNode
  //*
  //*****************************************/
  //
  //private void createZKNode(String znode, boolean canExist) {
  //  log.info("Trying to create znode "	+ znode + " (" + (canExist?"may":"must not")+" already exist)");
  //  try
  //  {
  //    zk.create(znode, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  //  }
  //  catch (KeeperException e)
  //  {
  //    if (canExist && (e.code() == KeeperException.Code.NODEEXISTS))
  //      {
  //        log.trace(znode+" already exists, this is OK");
  //      }
  //    else
  //      {
  //        log.info("Got "+e.getLocalizedMessage());
  //      }
  //  }
  //  catch (InterruptedException e)
  //  {
  //    log.info("Got "+e.getLocalizedMessage(), e);
  //  }
  //}

  //@Override
  //public void process(WatchedEvent event)
  //{
  //  log.trace("Got event : "+event);
  //  try
  //  {
  //    if (event.getType().equals(EventType.NodeChildrenChanged))
  //      {
  //        List<String> children = zk.getChildren(controlDir, this); // get the children and renew watch
  //        processChildren(children);
  //      }
  //  }
  //  catch (KeeperException | InterruptedException e)
  //  {
  //    log.error("Error processing extract", e);
  //  }
  //}
  
  /*****************************************
  *
  *  processChildren
  *
  *****************************************/
  @Override
  protected void processChildren(List<String> children) throws KeeperException, InterruptedException
  {
    if (!children.isEmpty())
      {
        Collections.sort(children); // we are getting an unsorted list
        for (String child : children)
          {
          String controlFile = controlDir + File.separator + child;
          String lockFile = lockDir + File.separator + child;
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
                  //de aici logica va merge pe JSON
                  String data = new String(d, utf8Charset);
                  log.info("Got data "+data);
                  JSONObject extractItemJSON = (JSONObject) (new JSONParser()).parse(data);
                  handleExtract(new ExtractItem(extractItemJSON));
                  extractManagerStatistics.incrementExtractCount();
                }
                catch (KeeperException | InterruptedException | NoSuchElementException e)
                {
                  log.error("Issue while reading from control node "+e.getLocalizedMessage(), e);
                  extractManagerStatistics.incrementFailureCount();
                }
                catch (IllegalCharsetNameException e)
                {
                  log.error("Unexpected issue, UTF-8 does not seem to exist "+e.getLocalizedMessage(), e);
                  extractManagerStatistics.incrementFailureCount();
                }
                catch (Exception e) // this is OK because we trace the root cause, and we'll fix it
                {
                  log.error("Unexpected issue " + e.getLocalizedMessage(), e);
                  extractManagerStatistics.incrementFailureCount();
                }
                finally 
                {
                  log.info("Deleting control "+controlFile);
                  try
                  {
                    zk.delete(controlFile, -1);
                  }
                  catch (KeeperException | InterruptedException e) 
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
                    catch (KeeperException | InterruptedException e)
                    {
                      log.info("Issue deleting lock : "+e.getLocalizedMessage(), e);
                    }
                  }
                }
              }
              catch (KeeperException | InterruptedException ignore)
              {
                // even so we check the existence of a lock, it could have been created in the mean time making create fail. We catch and ignore it.
                log.trace("Failed to create lock file, this is OK " +lockFile+ ":"+ignore.getLocalizedMessage(), ignore);
              } 
            } 
          else 
            {
              log.trace("--> This extract is already processed by another ReportManager instance");
            }
        }
      }
  }
  
  /*****************************************
  *
  *  handleReport
  *
  *****************************************/
  
  private void handleExtract(ExtractItem extractItem)
  {
    log.trace("---> Starting extract "+extractItem.getJSONObjectAsString());
    try 
    {
      String outputPath = Deployment.getExtractManagerOutputPath();
      log.trace("outputPath = "+outputPath);
      String dateFormat = Deployment.getExtractManagerDateFormat();
      log.trace("dateFormat = "+dateFormat);
      String fileExtension = Deployment.getExtractManagerFileExtension();
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
          + extractItem.getUserId()
          +"_"
          + extractItem.getExtractFileName()
          + "_"
          + fileSuffix
          + "."
          + fileExtension;
      log.trace("csvFilename = " + csvFilename);

      ExtractDriver ed = new ExtractDriver();
      try
      {
        ed.produceExtract(extractItem, zkHostList, brokerServers, esNode, csvFilename);
      }
      catch (Exception e)
      {
        // handle any kind of exception that can happen during generating the extract, and do not crash the container
        log.error("Exception processing extract " + extractItem.getExtractFileName() + " : " + e);
      }
      log.trace("---> Finished extract "+extractItem.getExtractFileName());
    }
    catch (SecurityException|IllegalArgumentException e)
    {
      log.error("Error : "+e.getLocalizedMessage(), e);
      extractManagerStatistics.incrementFailureCount();
    }
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) 
  {
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
      extractManagerStatistics = new ExtractManagerStatistics("extractmanager");
      ExtractManager rm = new ExtractManager();
      log.debug("ZK client created");
      while (true) 
        { //  sleep forever
          try 
          {
            Thread.sleep(Long.MAX_VALUE);
          } catch (InterruptedException ignore) {}
        }
    }
    catch (Exception e)
    {
      log.info("Issue in Zookeeper : "+e.getLocalizedMessage(), e);
    }
  }

}
