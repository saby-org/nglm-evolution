package com.evolving.nglm.evolution;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.UniqueKeyServer;

public class ZookeeperUniqueKeyServer
{

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ZookeeperUniqueKeyServer.class);
  
  //
  //  
  //

  private static final String UniqueKeyServerBasepath = "/uniqueKeyServer";
  private static final int LENGTH_OF_PREFIX = 3; // Number of digits to come from ZK
  private static final int TEN_POWER_LENGTH_OF_PREFIX = 1000; // 10^LENGTH_OF_PREFIX
  private final ZooKeeper zooKeeper;
  private UniqueKeyServer uniqueKeyServer;
  private long prefix;
  private String basePath;
  private String nodeName;
  
  public ZookeeperUniqueKeyServer(String groupID)
  {
    basePath = Deployment.getZookeeperRoot() + UniqueKeyServerBasepath;
    nodeName = basePath + File.separator + groupID;
    uniqueKeyServer = new UniqueKeyServer();
    zooKeeper = openZooKeeperAndCreateNode();
  }

  /**
   * Returns a key (a long) that is guaranteed to be different than any other key yet produced on any container.
   * 3 first characters come from zookeeper, and are incremented every time this class is instantiated, in any container.
   * 15 last chars are unique on a given instance of this class.
   * Note : if this class is instantiated more than one thousand times in a given deployment (with all instances active),
   * some keys might be reused.
   * @return A long that is guaranteed to be unique.
   */
  
  // No need to synchronise, uniqueKeyServer.getKey() is already synchronised
  public long getKey()
  {
    return Long.parseLong(getStringKey());
  }

  /**
   * Convenience method, similar to ketKey(), but returns a String (that represents a long), with no leading zeros.
   * getKey().toString() is always equals to getStringKey().
   * @return A String that is guaranteed to be unique.
   */
  
  // No need to synchronise, uniqueKeyServer.getKey() is already synchronised
  public String getStringKey()
  {
    long suffix = uniqueKeyServer.getKey() % 1_000_000_000_000_000L; // only get 15 least significant digits
    return String.format("%d%015d", prefix, suffix); // prefix is max 3 chars long, total is 18 digits, which fits in a long
  }
  
  /****************************************
  *
  *  openZooKeeperAndCreateNode
  *
  ****************************************/

  public ZooKeeper openZooKeeperAndCreateNode()
  {
    
    //
    //  open zookeeper
    //
    
    ZooKeeper zookeeper = null;
    while (zookeeper == null)
      {
        try
          {
            zookeeper = new ZooKeeper(System.getProperty("zookeeper.connect"), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, false);
          }
        catch (IOException e)
          {
            // ignore
          }
      }

    //
    //  ensure connected
    //

    ensureZooKeeper(zookeeper);
    
    //
    //  ensure basePath exists
    //
    
    try {
      zookeeper.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) // this error is OK
        {
          throw new ServerRuntimeException("zookeeper", e);
        }
    } catch (InterruptedException e) {
      log.error("openZooKeeperAndCreateNode() - lock() - Interrupted exception");
      throw new ServerRuntimeException("zookeeper", e);
    }

    try
    {
      //
      // find a sequential node that is not already used
      //
      boolean found = false;
      for (long i=0; i<TEN_POWER_LENGTH_OF_PREFIX; i++)
        {
          String path = nodeName + String.format("%010d", i); // same pattern as sequential nodes : groupID0000000000
          if (zookeeper.exists(path, false) == null)
            {
              //
              // Node does not exist, might be a good candidate.
              //

              try {
                //
                // Create ephemeral node
                //
                
                zookeeper.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); // ignore result
                prefix = i;
                found = true;
                break;
              }
              catch (KeeperException.NodeExistsException ex) // bad luck, node has been taken since we checked, just continue
              {
              }
            }
        }
      if (!found)
        {
          log.error("openZooKeeperAndCreateNode() - impossible to find an unused node after " + TEN_POWER_LENGTH_OF_PREFIX + " tries");
          throw new ServerRuntimeException("openZooKeeperAndCreateNode impossible to find an unused node");
        }
    }
    catch (KeeperException e)
      {
        log.error("openZooKeeperAndCreateNode() - create() - KeeperException code {}", e.code());
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (InterruptedException e)
      {
        log.error("openZooKeeperAndCreateNode() - create() - Interrupted exception");
        throw new ServerRuntimeException("zookeeper", e);
      }

    //
    //  return
    //

    return zookeeper;
  }

  /****************************************
  *
  *  closeZooKeeper
  *
  ****************************************/

  public void close()
  {
    //
    //  ensure connected
    //

    ensureZooKeeper(zooKeeper);

    //
    //  release group
    //

    // ephemeral node - will vanish on close

    //
    //  close
    //

    try
      {
        zooKeeper.close();
      }
    catch (InterruptedException e)
      {
        // ignore
      }
  }
  
  /****************************************
  *
  *  ensureZooKeeper
  *
  ****************************************/

  private static void ensureZooKeeper(ZooKeeper zookeeper)
  {
    //
    //  ensure connected
    //

    while (zookeeper.getState().isAlive() && ! zookeeper.getState().isConnected())
      {
        try { Thread.currentThread().sleep(200); } catch (InterruptedException ie) { }
      }

    //
    //  verify connected
    //

    if (! zookeeper.getState().isConnected())
      {
        throw new ServerRuntimeException("zookeeper unavailable");
      }
  }

  //
  // test code (to be removed ?)
  //
  
  public static void main(String args[])
  {
    ZookeeperUniqueKeyServer zuks = new ZookeeperUniqueKeyServer("evolution");
    IntStream.range(0,5).forEach(i->{long k=zuks.getKey();System.out.printf("Unique key #%d : %d\n",i,k);});
  }
}
