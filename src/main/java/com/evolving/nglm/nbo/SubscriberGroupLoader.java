/*****************************************************************************
*
*  SubscriberGroupLoader.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import com.rii.utilities.UniqueKeyServer;
import com.rii.utilities.SystemTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
   
public class SubscriberGroupLoader
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  LoadType
  //

  public enum LoadType
  {
    New("new", true),
    Delete("delete", false),
    Add("add", true),
    Remove("remove", false),
    Unknown("(unknown)", false);
    private String externalRepresentation;
    private boolean addRecord;
    private LoadType(String externalRepresentation, boolean addRecord) { this.externalRepresentation = externalRepresentation; this.addRecord = addRecord; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public boolean getAddRecord() { return addRecord; }
    public static LoadType fromExternalRepresentation(String externalRepresentation) { for (LoadType enumeratedValue : LoadType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberGroupLoader.class);

  //
  //  
  //

  private static final String SubscriberGroupEpochNodes = "/subscriberGroups/epochs/";
  private static final String SubscriberGroupLockNodes = "/subscriberGroups/locks/";
  
  //
  //  serdes
  //
  
  private static ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private static ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
  private static ConnectSerde<SubscriberGroupEpoch> subscriberGroupEpochSerde = SubscriberGroupEpoch.serde();

  /*****************************************
  *
  *  services
  *
  *****************************************/

  //
  //  epoch/uniqueKeyServer
  //
  
  private static UniqueKeyServer epochServer = new UniqueKeyServer();

  //
  //  subscriberIDService
  //

  private static SubscriberIDService subscriberIDService;
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    /****************************************
    *
    *  configuration
    *
    ****************************************/
    
    if (args.length < 4)
      {
        log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <groupName> (<displayName>) (<file>)");
        System.exit(-1);
      }

    String numThreadsArgument = args[0];
    String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
    String subscriberGroupAssignSubscriberIDTopic = Deployment.getSubscriberGroupAssignSubscriberIDTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String bootstrapServers = Deployment.getBrokerServers();
    String inputDirectoryName = "/app/data";
    String consumerGroupID = args[1];
    String loadTypeArgument = args[2];
    String groupName = args[3];
    LoadType loadType = LoadType.fromExternalRepresentation(loadTypeArgument);

    //
    //  setting display and fileName is dependent on loadType
    //  - ensure valid loadType
    //  - display is required on "New" load
    //  - no fileName on "Delete" load
    //
    
    String display = null;
    String fileName = null;
    switch (loadType)
      {
        case New:
          if (args.length < 6)
            {
              log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <groupName> (<displayName>) (<file>)");
              System.exit(-1);
            }
          display = args[4];
          fileName = args[5];
          break;
          
        case Add:
        case Remove:
          if (args.length < 5)
            {
              log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <groupName> (<displayName>) (<file>)");
              System.exit(-1);
            }
          fileName = args[4];
          break;
          
        case Delete:
          break;

        default:
          log.error("unknown load type {}; must be one of {}, {}, {}, {}", loadTypeArgument, LoadType.New.getExternalRepresentation(), LoadType.Delete.getExternalRepresentation(), LoadType.Add.getExternalRepresentation(), LoadType.New.getExternalRepresentation(), LoadType.Remove.getExternalRepresentation());
          System.exit(-1);
      }

    /****************************************
    *
    *  initialize
    *
    ****************************************/
    
    NGLMRuntime.initialize();
    log.info("main START: {} {} {} {} {} {} {} {} {}", numThreadsArgument, subscriberGroupTopic, subscriberGroupAssignSubscriberIDTopic, subscriberGroupEpochTopic, bootstrapServers, consumerGroupID, loadTypeArgument, groupName, (display != null ? display : ""), (fileName != null ? fileName : ""));
    
    /****************************************
    *
    *  validation
    *
    ****************************************/

    //
    //  validation -- numThreads
    //
    
    int numThreads;
    try
      {
        numThreads = Math.max(Integer.parseInt(numThreadsArgument),1);
      }
    catch (NumberFormatException e)
      {
        log.error("invalid number of threads: '{}'", numThreadsArgument);
        System.exit(-1);
      }

    //
    //  validation -- groupName
    //

    groupName = (groupName != null) ? groupName.toLowerCase() : groupName;
    
    //
    //  validation -- input file
    //

    File inputFile = null;
    switch (loadType)
      {
        case New:
        case Add:
        case Remove:
          if (fileName == null)
            {
              log.error("usage: SubscriberGroupLoader requires input file for requested operation");
              System.exit(-1);
            }
          inputFile = new File(inputDirectoryName, fileName);
          if (! inputFile.exists())
            {
              log.error("input file {} does not exist", fileName);
              System.exit(-1);
            }
          break;
          
        case Delete:
          break;

        default:
          throw new ServerRuntimeException("unknown loadType");
      }

    /*****************************************
    *
    *  subscriberIDService
    *
    *****************************************/

    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels());
    
    /*****************************************
    *
    *  open zookeeper and lock group
    *
    *****************************************/

    ZooKeeper zookeeper = openZooKeeperAndLockGroup(groupName);
    
    /*****************************************
    *
    *  retrieve existing epoch
    *
    *****************************************/

    SubscriberGroupEpoch existingEpoch = retrieveSubscriberGroupEpoch(zookeeper, groupName, loadType, display);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    int epoch;
    switch (loadType)
      {
        case New:
        case Delete:
          epoch = existingEpoch.getEpoch() + 1;
          break;
          
        case Add:
        case Remove:
          epoch = existingEpoch.getEpoch();
          break;

        default:
          throw new ServerRuntimeException("unknown loadType");
      }
    
    /*****************************************
    *
    *  kafka producer
    *
    *****************************************/

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);

    /*****************************************
    *
    *  submit new epoch (if necessary)
    *
    *****************************************/

    if (existingEpoch.getEpoch() != epoch)
      {
        updateSubscriberGroupEpoch(zookeeper, existingEpoch, epoch, (loadType != LoadType.Delete), kafkaProducer, subscriberGroupEpochTopic);
      }

    /*****************************************
    *
    *  submit all records from file (if necessary)
    *
    *****************************************/

    if (inputFile != null)
      {
        log.info("updating subscribers in group {}", groupName);
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile)))
          {
            while (true)
              {
                //
                //  read line
                //

                String msisdn = reader.readLine();
                if (msisdn == null) break;

                //
                //  resolve subscriberID
                //

                String subscriberID = null;
                try
                  {
                    subscriberID = subscriberIDService.getSubscriberID("msisdn", msisdn);
                  }
                catch (SubscriberIDService.SubscriberIDServiceException e)
                  {
                    // ignore
                  }

                //
                //  submit message
                //

                String topic = (subscriberID != null) ? subscriberGroupTopic : subscriberGroupAssignSubscriberIDTopic;
                SubscriberGroup subscriberGroup = new SubscriberGroup((subscriberID != null ? subscriberID : msisdn), now, groupName, epoch, loadType.getAddRecord());
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(topic, stringKeySerde.serializer().serialize(topic, new StringKey(subscriberGroup.getSubscriberID())), subscriberGroupSerde.serializer().serialize(topic, subscriberGroup)));
              }
          }
      }
    kafkaProducer.flush();
    log.info("group {} updated", groupName);
    
    /*****************************************
    *
    *  close
    *
    *****************************************/

    closeZooKeeperAndReleaseGroup(zookeeper, groupName);
    subscriberIDService.close();
  }

  /****************************************
  *
  *  openZooKeeperAndLockGroup
  *
  ****************************************/

  private static ZooKeeper openZooKeeperAndLockGroup(String groupName)
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
        zookeeper.create(Deployment.getZookeeperRoot() + SubscriberGroupLockNodes + groupName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }
    catch (KeeperException.NodeExistsException e)
      {
        log.error("subscriber group {} currently being updated", groupName);
        System.exit(-1);
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

  private static void closeZooKeeperAndReleaseGroup(ZooKeeper zookeeper, String groupName)
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

  private static SubscriberGroupEpoch retrieveSubscriberGroupEpoch(ZooKeeper zookeeper, String groupName, LoadType loadType, String display)
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
    String node = Deployment.getZookeeperRoot() + SubscriberGroupEpochNodes + groupName;
    if (! subscriberGroupEpochNodeExists)
      {
        //
        //  read existing node
        //

        try
          {
            subscriberGroupEpochNodeExists = (zookeeper.exists(node, false) != null);
            if (!subscriberGroupEpochNodeExists) log.info("subscriberGroupEpoch node {} does not exist", groupName);
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
        //  ensure exists (if not new)
        //

        if (! subscriberGroupEpochNodeExists && loadType != LoadType.New)
          {
            throw new ServerRuntimeException("no such group");
          }
        
        //
        //  create subscriberGroupEpoch node (if necessary)
        //

        if (! subscriberGroupEpochNodeExists)
          {
            log.info("retrieveSubscriberGroupEpoch() - creating node {}", groupName);
            try
              {
                SubscriberGroupEpoch newSubscriberGroupEpoch = new SubscriberGroupEpoch(groupName, display);
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

  private static void updateSubscriberGroupEpoch(ZooKeeper zookeeper, SubscriberGroupEpoch existingSubscriberGroupEpoch, int epoch, boolean active, KafkaProducer<byte[], byte[]> kafkaProducer, String subscriberGroupEpochTopic)
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

    SubscriberGroupEpoch subscriberGroupEpoch = new SubscriberGroupEpoch(existingSubscriberGroupEpoch.getGroupName(), epoch, existingSubscriberGroupEpoch.getDisplay(), active);
    String node = Deployment.getZookeeperRoot() + SubscriberGroupEpochNodes + subscriberGroupEpoch.getGroupName();
    try
      {
        JSONObject jsonSubscriberGroupEpoch = subscriberGroupEpoch.getJSONRepresentation();
        String stringSubscriberGroupEpoch = jsonSubscriberGroupEpoch.toString();
        byte[] rawSubscriberGroupEpoch = stringSubscriberGroupEpoch.getBytes(StandardCharsets.UTF_8);
        zookeeper.setData(node, rawSubscriberGroupEpoch, existingSubscriberGroupEpoch.getZookeeperVersion());
      }
    catch (KeeperException.BadVersionException e)
      {
        log.error("concurrent write aborted for subscriberGroupEpoch {}", subscriberGroupEpoch.getGroupName());
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

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(subscriberGroupEpochTopic, stringKeySerde.serializer().serialize(subscriberGroupEpochTopic, new StringKey(subscriberGroupEpoch.getGroupName())), subscriberGroupEpochSerde.serializer().serialize(subscriberGroupEpochTopic, subscriberGroupEpoch)));
    
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("updateSubscriberGroupEpoch() - updated group {}", subscriberGroupEpoch.getGroupName());
  }
}
