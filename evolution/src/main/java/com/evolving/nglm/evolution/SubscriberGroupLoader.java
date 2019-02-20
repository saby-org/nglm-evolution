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
import com.evolving.nglm.core.UniqueKeyServer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
  private static SegmentationDimensionService segmentationDimensionService;
  private static UniqueKeyServer uniqueKeyServer = new UniqueKeyServer();
  
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
        log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <dimensionName> (<file>)");
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
    String dimensionName = args[3];
    LoadType loadType = LoadType.fromExternalRepresentation(loadTypeArgument);

    //
    //  setting fileName is dependent on loadType
    //  - ensure valid loadType
    //  - no fileName on "Delete" load
    //
    
    String fileName = null;
    switch (loadType)
      {
        case New:
        case Add:
        case Remove:
          if (args.length < 5)
            {
              log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <dimensionName> (<file>)");
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
    log.info("main START: {} {} {} {} {} {} {} {}", numThreadsArgument, subscriberGroupTopic, subscriberGroupAssignSubscriberIDTopic, subscriberGroupEpochTopic, bootstrapServers, consumerGroupID, loadTypeArgument, dimensionName, (fileName != null ? fileName : ""));
    
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
    *  services
    *
    *****************************************/

    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels());
    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "subscribergrouploader-" + Long.toString(uniqueKeyServer.getKey()), Deployment.getSegmentationDimensionTopic(), false);

    /*****************************************
    *
    *  retrieve dimension
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    SegmentationDimension dimension = null;
    for (SegmentationDimension candidateDimension : segmentationDimensionService.getActiveSegmentationDimensions(now))
      {
        if (Objects.equals(candidateDimension.getSegmentationDimensionName(), dimensionName))
          {
            dimension = candidateDimension;
            break;
          }
      }
    if (dimension == null)
      {
        log.error("dimension {} does not exist", dimensionName);
        System.exit(-1);
      }
    
    /*****************************************
    *
    *  dimensionID
    *
    *****************************************/
    
    String dimensionID = dimension.getSegmentationDimensionID();
    
    /*****************************************
    *
    *  open zookeeper and lock dimension
    *
    *****************************************/

    ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(dimensionID);
    
    /*****************************************
    *
    *  retrieve existing epoch
    *
    *****************************************/

    SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, dimensionID, loadType, dimension.getSegmentationDimensionName());

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

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

        log.info("updating subscribers in dimension {}", dimensionName);
        if (useAlternateID)
          log.info("using alternate id {} {}", Deployment.getSubscriberGroupLoaderAlternateID(), (String) (subscriberGroupAssignSubscriberIDTopic != null ? "with autoprovision" : ""));
        else
          log.info("using insternal subscriber ids");

        /*****************************************
        *
        *  segments by name
        *
        *****************************************/

        Map<String,String> segmentsByName = new HashMap<String,String>();
        for (Segment segment : dimension.getSegments())
          {
            segmentsByName.put(segment.getName(), segment.getID());
          }
        
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

                String line = reader.readLine();
                if (line == null)
                  {
                    break;
                  }

                //
                //  parse
                //

                String[] infos = line.split("[|]"); //TODO SCH : make the separator configurable
                if (infos.length != 2)
                  {
                    log.warn("bad line {} - {} tokens - skipping", line, infos.length);
                    continue;
                  }
                String subscriberID = infos[0];
                String segmentName = infos[1];

                //
                //  normalize
                //

                String segmentID = (segmentName != null) ? segmentsByName.get(segmentName) : null;
                if (segmentID == null) 
                  {
                    log.warn("unknown segment {} specified for subscriber {} - skipping", segmentName, subscriberID);
                    continue;
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
                    SubscriberGroup subscriberGroup = new SubscriberGroup(effectiveSubscriberID, now, dimensionID, segmentID, epoch, loadType.getAddRecord());
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
    log.info("dimension {} with dimensionID {} updated", dimensionName, dimensionID);
    
    /*****************************************
    *
    *  close
    *
    *****************************************/

    SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, dimensionID);
    subscriberIDService.close();
  }
}
