/*****************************************************************************
*
*  SubscriberGroupEpochService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.SubscriberGroupLoader.LoadType;

public class SubscriberGroupEpochService
{
  
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberGroupEpochService.class);
  
  //
  //  
  //

  private static final String SubscriberGroupEpochNodes = "/subscriberGroups/epochs/";
  private static final String SubscriberGroupLockNodes = "/subscriberGroups/locks/";

  //
  //  serdes
  //
  
  private static ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private static ConnectSerde<SubscriberGroupEpoch> subscriberGroupEpochSerde = SubscriberGroupEpoch.serde();
  
  /****************************************
  *
  *  openZooKeeperAndLockGroup
  *
  ****************************************/

  public static ZooKeeper openZooKeeperAndLockGroup(String primaryID)
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
    //  lock group
    //

    try
      {
        zookeeper.create(Deployment.getZookeeperRoot() + SubscriberGroupLockNodes + primaryID, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }
    catch (KeeperException.NodeExistsException e)
      {
        log.error("subscriber group {} currently being updated", primaryID);
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (KeeperException e)
      {
        log.error("openZooKeeperAndLockGroup() - lock() - KeeperException code {}", e.code());
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (InterruptedException e)
      {
        log.error("openZooKeeperAndLockGrou() - lock() - Interrupted exception");
        throw new ServerRuntimeException("zookeeper", e);
      }

    //
    //  return
    //

    return zookeeper;
  }

  /****************************************
  *
  *  closeZooKeeperAndReleaseGroup
  *
  ****************************************/

  public static void closeZooKeeperAndReleaseGroup(ZooKeeper zookeeper, String primaryID)
  {
    //
    //  ensure connected
    //

    ensureZooKeeper(zookeeper);

    //
    //  release group
    //

    // ephemeral node - will vanish on close

    //
    //  close
    //

    try
      {
        zookeeper.close();
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
  
  /****************************************
  *
  *  retrieveSubscriberGroupEpoch
  *
  ****************************************/

  public static SubscriberGroupEpoch retrieveSubscriberGroupEpoch(ZooKeeper zookeeper, String primaryID)
  {
    /*****************************************
    *
    *  zookeeper
    *
    *****************************************/

    //
    //  ensure connected
    //

    ensureZooKeeper(zookeeper);

    //
    //  ensure subscriberGroupEpoch node exists
    //

    boolean subscriberGroupEpochNodeExists = false;
    String node = Deployment.getZookeeperRoot() + SubscriberGroupEpochNodes + primaryID;
    if (! subscriberGroupEpochNodeExists)
      {
        //
        //  read existing node
        //

        try
          {
            subscriberGroupEpochNodeExists = (zookeeper.exists(node, false) != null);
            if (!subscriberGroupEpochNodeExists) if (log.isDebugEnabled()) log.debug("subscriberGroupEpoch node with ID {} does not exist", primaryID);
          }
        catch (KeeperException e)
          {
            log.error("retrieveSubscriberGroupEpoch() - exists() - KeeperException code {}", e.code());
            throw new ServerRuntimeException("zookeeper", e);
          }
        catch (InterruptedException e)
          {
            log.error("retrieveSubscriberGroupEpoch() - exists() - Interrupted exception");
            throw new ServerRuntimeException("zookeeper", e);
          }

        //
        //  create subscriberGroupEpoch node (if necessary)
        //

        if (! subscriberGroupEpochNodeExists)
          {
            log.info("retrieveSubscriberGroupEpoch() - creating node {}", primaryID);
            try
              {
                SubscriberGroupEpoch newSubscriberGroupEpoch = new SubscriberGroupEpoch(primaryID);
                JSONObject jsonNewSubscriberGroupEpoch = newSubscriberGroupEpoch.getJSONRepresentation();
                String stringNewSubscriberGroupEpoch = jsonNewSubscriberGroupEpoch.toString();
                byte[] rawNewSubscriberGroupEpoch = stringNewSubscriberGroupEpoch.getBytes(StandardCharsets.UTF_8);
                zookeeper.create(node, rawNewSubscriberGroupEpoch, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                subscriberGroupEpochNodeExists = true;
              }
            catch (KeeperException.NodeExistsException e)
              {
                subscriberGroupEpochNodeExists = true;
              }
            catch (KeeperException e)
              {
                log.error("retrieveSubscriberGroupEpoch() - create() - KeeperException code {}", e.code());
                throw new ServerRuntimeException("zookeeper", e);
              }
            catch (InterruptedException e)
              {
                log.error("retrieveSubscriberGroupEpoch() - exists() - Interrupted exception");
                throw new ServerRuntimeException("zookeeper", e);
              }
          }
      }
    
    //
    //  read subscriberGroupEpoch
    //

    SubscriberGroupEpoch subscriberGroupEpoch = null;
    try
      {
        Stat stat = new Stat();
        byte[] rawSubscriberGroupEpoch = zookeeper.getData(node, null, stat);
        String stringSubscriberGroupEpoch = new String(rawSubscriberGroupEpoch, StandardCharsets.UTF_8);
        JSONObject jsonSubscriberGroupEpoch = (JSONObject) (new JSONParser()).parse(stringSubscriberGroupEpoch);
        subscriberGroupEpoch = new SubscriberGroupEpoch(jsonSubscriberGroupEpoch, stat.getVersion());
      }
    catch (KeeperException e)
      {
        log.error("retrieveSubscriberGroupEpoch() - getData() - KeeperException code {}", e.code());
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (InterruptedException|ParseException e)
      {
        log.error("retrieveSubscriberGroupEpoch() - getData() - Exception");
        throw new ServerRuntimeException("zookeeper", e);
      }

    //
    //  return
    //

    return subscriberGroupEpoch;
  }

  /****************************************
  *
  *  updateSubscriberGroupEpoch
  *
  ****************************************/

  public static SubscriberGroupEpoch updateSubscriberGroupEpoch(ZooKeeper zookeeper, String primaryID, SubscriberGroupEpoch existingSubscriberGroupEpoch, KafkaProducer<byte[], byte[]> kafkaProducer, String subscriberGroupEpochTopic)
  {
    /*****************************************
    *
    *  zookeeper
    *
    *****************************************/

    //
    //  ensure connected
    //

    ensureZooKeeper(zookeeper);

    //
    //  update subscriberGroupEpoch node
    //

    SubscriberGroupEpoch subscriberGroupEpoch = new SubscriberGroupEpoch(existingSubscriberGroupEpoch);
    String node = Deployment.getZookeeperRoot() + SubscriberGroupEpochNodes + subscriberGroupEpoch.getPrimaryID();
    try
      {
        JSONObject jsonSubscriberGroupEpoch = subscriberGroupEpoch.getJSONRepresentation();
        String stringSubscriberGroupEpoch = jsonSubscriberGroupEpoch.toString();
        byte[] rawSubscriberGroupEpoch = stringSubscriberGroupEpoch.getBytes(StandardCharsets.UTF_8);
        zookeeper.setData(node, rawSubscriberGroupEpoch, existingSubscriberGroupEpoch.getZookeeperVersion());
      }
    catch (KeeperException.BadVersionException e)
      {
        log.error("concurrent write aborted for subscriberGroupEpoch {}", primaryID);
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (KeeperException e)
      {
        log.error("setData() - KeeperException code {}", e.code());
        throw new ServerRuntimeException("zookeeper", e);
      }
    catch (InterruptedException e)
      {
        log.error("setData() - InterruptedException");
        throw new ServerRuntimeException("zookeeper", e);
      }
    
    /*****************************************
    *
    *  kafka
    *
    *****************************************/

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(subscriberGroupEpochTopic, stringKeySerde.serializer().serialize(subscriberGroupEpochTopic, new StringKey(subscriberGroupEpoch.getPrimaryID())), subscriberGroupEpochSerde.serializer().serialize(subscriberGroupEpochTopic, subscriberGroupEpoch)));
    
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("updateSubscriberGroupEpoch() - updated group {}", subscriberGroupEpoch.getPrimaryID());

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberGroupEpoch;
  }
}
