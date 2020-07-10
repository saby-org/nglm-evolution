package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.TargetService;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ExtractService
{
  /*****************************************
   *
   *  configuration
   *
   *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ExtractService.class);

  private static ZooKeeper zk = null;
  private static final int NB_TIMES_TO_TRY = 10;

  /**
   * Put an extract in zookeeper for processing. Is called by GUImanager
   * @param extractItem   contains all information needed by an extract. The object in serialized as JSON and put as content for zookeeper node
   * @see ExtractItem
   */
  public static void launchGenerateExtract(ExtractItem extractItem)
  {
    log.trace("launchTarget : " + extractItem.getExtractName());
    String znode = ExtractManager.getControlDir()+ File.separator + "launchTarget-" + extractItem.getExtractName()+"-"+extractItem.getUserId() + "-";
    if (getZKConnection())
      {
        log.debug("Trying to create ephemeral znode " + znode + " for " + extractItem.getExtractName());
        try
          {
            // Create new file in control dir with reportName inside, to trigger
            // report generation
            //ExtractItem extractItem = new ExtractItem(target.getTargetName(),target.getTargetingCriteria(),null);
            zk.create(znode, extractItem.getJSONObjectAsString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
          }
        catch (KeeperException e)
          {
            log.info("Got " + e.getLocalizedMessage());
          }
        catch (InterruptedException e)
          {
            log.info("Got " + e.getLocalizedMessage());
          }
      }
    else
      {
        log.info("There was a major issue connecting to zookeeper");
      }
  }

  private static boolean isConnectionValid(ZooKeeper zookeeper)
  {
    return (zookeeper != null) && (zookeeper.getState() == ZooKeeper.States.CONNECTED);
  }

  /**
   * Identify if an extract is in process
   * @param extractName   Identify the name how this was passed at launch.
   * @return              true if extract is in processing
   */
  public static boolean isExtractRunning (String extractName) {
    try
      {
        if (getZKConnection())
          {
            List<String> children = zk.getChildren(ExtractManager.getControlDir(), false);
            for(String child : children)
              {
                if(child.contains(extractName))
                  {
                    String znode = ExtractManager.getControlDir() + File.separator+child;
                    return (zk.exists(znode, false) != null);
                  }
              }
          }
        else
          {
            log.info("There was a major issue connecting to zookeeper");
          }
      }
    catch (KeeperException e)
      {
        log.info(e.getLocalizedMessage());
      }
    catch (InterruptedException e) { }
    return false;
  }

  /**
   * verify if an zk connection is available. If the connection is not valid the method will try to reinitialize the connection
   * If connection succeed before number of tries the methd return true and zk variable will store new valid connection\
   * if connection not succeed in number of retries method will return false
   * @see     ExtractService#NB_TIMES_TO_TRY
   * @return  true if connection valid or reinitialized. false if reinitialization fails after number of retries
   */
  private static boolean getZKConnection()  {
    if (!isConnectionValid(zk)) {
      log.debug("Trying to acquire ZooKeeper connection to "+ Deployment.getZookeeperConnect());
      int nbLoop = 0;
      do
        {
          try
            {
              zk = new ZooKeeper(Deployment.getZookeeperConnect(), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, false);
              try { Thread.sleep(1*1000); } catch (InterruptedException e) {}
              if (!isConnectionValid(zk))
                {
                  log.info("Could not get a zookeeper connection, waiting... ("+(NB_TIMES_TO_TRY-nbLoop)+" more times to go)");
                  try { Thread.sleep(5*1000); } catch (InterruptedException e) {}
                }
            }
          catch (IOException e)
            {
              log.info("could not create zookeeper client using {}", Deployment.getZookeeperConnect());
            }
        }
      while (!isConnectionValid(zk) && (nbLoop++ < NB_TIMES_TO_TRY));
    }
    return isConnectionValid(zk);
  }

}
