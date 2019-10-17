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
import com.evolving.nglm.evolution.SubscriberGroup.SubscriberGroupType;

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
import java.util.Arrays;
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
    SubscriberGroupType subscriberGroupType = SubscriberGroupType.fromExternalRepresentation(args[3]);
    String groupName = args[4];
    LoadType loadType = LoadType.fromExternalRepresentation(loadTypeArgument);

    //
    //  grouptype is valid
    //

    switch (subscriberGroupType)
      {
        case SegmentationDimension:
        case Target:
        case ExclusionInclusionTarget:
          break;

        default:
          log.error("unknown subscriber group type {}; must be one of {}, {}", args[3], SubscriberGroupType.SegmentationDimension.getExternalRepresentation(), SubscriberGroupType.Target.getExternalRepresentation());
          System.exit(-1);
      }
        

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
          if (args.length < 6)
            {
              log.error("usage: SubscriberGroupLoader <new|delete|add|remove> <segmentationDimension|target> <groupName> (<file>)");
              System.exit(-1);
            }
          fileName = args[5];
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
    
    NGLMRuntime.initialize(true);
    log.info("main START: {} {} {} {} {} {} {} {}", numThreadsArgument, subscriberGroupTopic, subscriberGroupAssignSubscriberIDTopic, subscriberGroupEpochTopic, bootstrapServers, consumerGroupID, loadTypeArgument, groupName, (fileName != null ? fileName : ""));
    
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

    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "subscribergrouploader");

    /*****************************************
    *
    *  retrieve dimension/target
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    SegmentationDimension dimension = null;
    Target target = null;
    String primaryID = null;
    switch (subscriberGroupType)
      {
        case SegmentationDimension:
          SegmentationDimensionService segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "subscribergrouploader-segmentationdimension-" + Long.toString(uniqueKeyServer.getKey()), Deployment.getSegmentationDimensionTopic(), false);
          for (SegmentationDimension candidateDimension : segmentationDimensionService.getActiveSegmentationDimensions(now))
            {
              if (Objects.equals(candidateDimension.getSegmentationDimensionName(), groupName))
                {
                  dimension = candidateDimension;
                  break;
                }
            }
          if (dimension == null)
            {
              log.error("dimension {} does not exist", groupName);
              System.exit(-1);
            }
          primaryID = dimension.getSegmentationDimensionID();
          break;

        case Target:
          TargetService targetService = new TargetService(bootstrapServers, "subscribergrouploader-target-" + Long.toString(uniqueKeyServer.getKey()), Deployment.getTargetTopic(), false);
          for (Target candidateTarget : targetService.getActiveTargets(now))
            {
              if (Objects.equals(candidateTarget.getTargetName(), groupName))
                {
                  target = candidateTarget;
                  break;
                }
            }
          if (target == null)
            {
              log.error("target {} does not exist", groupName);
              System.exit(-1);
            }
          primaryID = target.getTargetID();
          break;
      }
    
    /*****************************************
    *
    *  open zookeeper and lock dimension
    *
    *****************************************/

    ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(primaryID);
    
    /*****************************************
    *
    *  retrieve existing epoch
    *
    *****************************************/

    SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, primaryID);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    boolean updateEpoch = false;
    switch (loadType)
      {
        case New:
        case Delete:
          updateEpoch = true;
          break;
          
        case Add:
        case Remove:
          updateEpoch = false;
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

    /*************a****************************
    *
    *  submit new epoch (if necessary)
    *
    *****************************************/

    SubscriberGroupEpoch subscriberGroupEpoch = existingEpoch;
    if (updateEpoch)
      {
        subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, primaryID, existingEpoch, kafkaProducer, subscriberGroupEpochTopic);
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
          log.info("using internal subscriber ids");

        /*****************************************
        *
        *  segments by name
        *
        *****************************************/

        Map<String,String> segmentsByName = new HashMap<String,String>();
        switch (subscriberGroupType)
          {
            case SegmentationDimension:
              for (Segment segment : dimension.getSegments())
                {
                  segmentsByName.put(segment.getName(), segment.getID());
                }
              break;
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

                String subscriberID = null;
                String segmentName = null;
                String segmentID = null;
                switch (subscriberGroupType)
                  {
                    case SegmentationDimension:
                      String[] infos = line.split("[|]"); //TODO SCH : make the separator configurable
                      if (infos.length != 2)
                        {
                          log.warn("bad line {} - {} tokens - skipping", line, infos.length);
                          continue;
                        }
                      subscriberID = infos[0].trim();
                      segmentName = infos[1].trim();
                      segmentID = (segmentName != null) ? segmentsByName.get(segmentName) : null;
                      if (segmentID == null) 
                        {
                          log.warn("unknown segment {} specified for subscriber {} - skipping", segmentName, subscriberID);
                          continue;
                        }
                      break;

                    case Target:
                      subscriberID = line.trim();
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
                    SubscriberGroup subscriberGroup = null;
                    switch (subscriberGroupType)
                      {
                        case SegmentationDimension:
                          subscriberGroup = new SubscriberGroup(effectiveSubscriberID, now, SubscriberGroupType.SegmentationDimension, Arrays.asList(primaryID, segmentID), subscriberGroupEpoch.getEpoch(), loadType.getAddRecord());
                          break;
                        case Target:
                          subscriberGroup = new SubscriberGroup(effectiveSubscriberID, now, SubscriberGroupType.Target, Arrays.asList(primaryID), subscriberGroupEpoch.getEpoch(), loadType.getAddRecord());
                          break;
                      }
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
    log.info("group {} with ID {} updated", groupName, primaryID);
    
    /*****************************************
    *
    *  close
    *
    *****************************************/

    SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, primaryID);
    subscriberIDService.close();
  }
}
