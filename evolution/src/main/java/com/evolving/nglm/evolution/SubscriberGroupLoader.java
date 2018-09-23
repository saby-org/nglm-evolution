/*****************************************************************************
*
*  SubscriberGroupLoader.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.SystemTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
   
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
  //  serdes
  //
  
  private static ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private static ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
  
  /*****************************************
  *
  *  services
  *
  *****************************************/

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

    ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(groupName);
    
    /*****************************************
    *
    *  retrieve existing epoch
    *
    *****************************************/

    SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, groupName, loadType, display);

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
        SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, existingEpoch, epoch, (loadType != LoadType.Delete), kafkaProducer, subscriberGroupEpochTopic);
      }

    /*****************************************
    *
    *  submit all records from file (if necessary)
    *
    *****************************************/

    boolean useAlternateID = Deployment.getSubscriberGroupLoaderAlternateID() != null;
    boolean useAutoProvision = useAlternateID && Objects.equals(Deployment.getSubscriberGroupLoaderAlternateID(), Deployment.getExternalSubscriberID());
    if (inputFile != null)
      {
        /*****************************************
        *
        *  log
        *
        *****************************************/

        log.info("updating subscribers in group {}", groupName);
        if (useAlternateID)
          log.info("using alternate id {} {}", Deployment.getSubscriberGroupLoaderAlternateID(), (String) (subscriberGroupAssignSubscriberIDTopic != null ? "with autoprovision" : ""));
        else
          log.info("using insternal subscriber ids");

        /*****************************************
        *
        *  process file
        *
        *****************************************/

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile)))
          {
            while (true)
              {
                //
                //  read line
                //

                String subscriberID = reader.readLine();
                if (subscriberID == null)
                  {
                    break;
                  }

                //
                //  resolve subscriberID (if necessary)
                //
                
                String resolvedSubscriberID = null;
                if (useAlternateID)
                  {
                    try
                      {
                        resolvedSubscriberID = subscriberIDService.getSubscriberID(Deployment.getSubscriberGroupLoaderAlternateID(), subscriberID);
                      }
                    catch (SubscriberIDService.SubscriberIDServiceException e)
                      {
                        StringWriter stackTraceWriter = new StringWriter();
                        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                        log.error(stackTraceWriter.toString());
                        break;
                      }
                  }

                //
                //  cases
                //

                String effectiveSubscriberID;
                boolean autoProvision;
                if (useAlternateID && resolvedSubscriberID != null)
                  {
                    effectiveSubscriberID = resolvedSubscriberID;
                    autoProvision = false;
                  }
                else if (useAlternateID && resolvedSubscriberID == null && useAutoProvision)
                  {
                    effectiveSubscriberID = subscriberID;
                    autoProvision = true;
                  }
                else if (useAlternateID && resolvedSubscriberID == null && ! useAutoProvision)
                  {
                    effectiveSubscriberID = null;
                    autoProvision = false;
                  }
                else
                  {
                    effectiveSubscriberID = subscriberID;
                    autoProvision = false;
                  }

                //
                //  submit message (if appropriate)
                //

                if (effectiveSubscriberID != null)
                  {
                    String topic = autoProvision ? subscriberGroupAssignSubscriberIDTopic : subscriberGroupTopic;
                    SubscriberGroup subscriberGroup = new SubscriberGroup(effectiveSubscriberID, now, groupName, epoch, loadType.getAddRecord());
                    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(topic, stringKeySerde.serializer().serialize(topic, new StringKey(subscriberGroup.getSubscriberID())), subscriberGroupSerde.serializer().serialize(topic, subscriberGroup)));
                  }
                else
                  {
                    log.info("could not process subscriber identified by {}", subscriberID);
                  }
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

    SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, groupName);
    subscriberIDService.close();
  }
}
