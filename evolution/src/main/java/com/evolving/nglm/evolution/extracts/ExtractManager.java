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
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class uses Zookeeper to launch the generation of extract.
 * When a node is created in the controlDir, it triggers the generation of a extract.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report (of the same type) to be created. 
 *
 */
public class ExtractManager extends ReportManager
{

  private static final Logger log = LoggerFactory.getLogger(ExtractManager.class);
  private static ExtractManagerStatistics extractManagerStatistics;
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
  
  /*****************************************
  *
  *  override processChildren that is used in watcher event
   * @see ReportManager#process(WatchedEvent)
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
          ExtractLauncher extractLauncer = new ExtractLauncher(zk,controlDir,lockDir,child,zkHostList,brokerServers,esNode,dfrm,extractManagerStatistics);
          extractLauncer.start();
        }
      }
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) 
  {
    log.info("ExtractManager: received " + args.length + " args");
    for(String arg : args)
      {
        log.info("ExtractManager main : arg " + arg);
      }
    if (args.length < 5) 
      {
        log.error("Usage : ExtractManager BrokerServers ESNode replication partitions standby");
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
      ExtractManager extractManager = new ExtractManager();
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
