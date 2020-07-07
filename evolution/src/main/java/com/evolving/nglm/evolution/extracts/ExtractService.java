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

  /*
   * Called by GuiManager to trigger a one-time report
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

  public static boolean isTargetExtractRunning(String targetName) {
    try
      {
        if (getZKConnection())
          {
            List<String> children = zk.getChildren(ExtractManager.getControlDir(), false);
            for(String child : children)
              {
                if(child.contains(targetName))
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
