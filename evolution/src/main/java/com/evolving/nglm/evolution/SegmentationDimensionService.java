/****************************************************************************
*
*  SegmentationDimensionService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;
import com.evolving.nglm.evolution.SubscriberGroup.SubscriberGroupType;
import com.evolving.nglm.evolution.SubscriberGroupLoader.LoadType;

public class SegmentationDimensionService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentationDimensionService.class);
  
  //
  //  serdes
  //
  
  private static ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private static ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String,Segment> segmentsByID = new HashMap<String,Segment>();
  private volatile boolean stopRequested = false;
  private BlockingQueue<SegmentationDimensionFileImport> listenerQueue = new LinkedBlockingQueue<SegmentationDimensionFileImport>();
  private Thread listenerThread = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private UploadedFileService uploadedFileService = null; 
  private SubscriberIDService subscriberIDService = null;
  private String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService, SegmentationDimensionListener segmentationDimensionListener, boolean notifyOnSignificantChange)
  {
    //
    //  super
    //  

    super(bootstrapServers, "segmentationDimensionService", groupID, segmentationDimensionTopic, masterService, getSuperListener(segmentationDimensionListener), "putSegmentationDimension", "removeSegmentationDimension", notifyOnSignificantChange);

    //
    //  listener
    //

    GUIManagedObjectListener segmentListener = new GUIManagedObjectListener()
    {
      //
      //  guiManagedObjectActivated
      //

      @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject)
      {
        SegmentationDimension segmentationDimension = (SegmentationDimension) guiManagedObject;
        for (Segment segment : segmentationDimension.getSegments())
          {
            segmentsByID.put(segment.getID(), segment);
          }
      }

      //
      //  guiManagedObjectDeactivated
      //

      @Override public void guiManagedObjectDeactivated(String guiManagedObjectID)
      {
        //
        //  ignore
        //
      }
    };

    //
    //  register
    //

    registerListener(segmentListener);

    //
    //  initialize segmentsByID
    //

    Date now = SystemTime.getCurrentTime();
    for (SegmentationDimension segmentationDimension : getActiveSegmentationDimensions(now))
      {
        for (Segment segment : segmentationDimension.getSegments())
          {
            segmentsByID.put(segment.getID(), segment);
          }
      }
    
    /*****************************************
    *
    *  kafka producer
    *
    *****************************************/

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    //
    //  listener
    //

    Runnable listener = new Runnable() { @Override public void run() { runListener(); } };
    listenerThread = new Thread(listener, "SegmentationDimensionService");
    listenerThread.start();
  }

  //
  //  constructor
  //

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService, SegmentationDimensionListener segmentationDimensionListener)
  {
    this(bootstrapServers, groupID, segmentationDimensionTopic, masterService, segmentationDimensionListener, true);
  }

  //
  //  constructor
  //

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, segmentationDimensionTopic, masterService, (SegmentationDimensionListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SegmentationDimensionListener segmentationDimensionListener)
  {
    GUIManagedObjectListener superListener = null;
    if (segmentationDimensionListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { segmentationDimensionListener.segmentationDimensionActivated((SegmentationDimension) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { segmentationDimensionListener.segmentationDimensionDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
   *
   *  getSummaryJSONRepresentation
   *
   *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("status",guiManagedObject.getJSONRepresentation().get("status"));
    result.put("targetingType", guiManagedObject.getJSONRepresentation().get("targetingType"));
    result.put("noOfSegments",guiManagedObject.getJSONRepresentation().get("numberOfSegments"));
    return result;
  }

  /*****************************************
  *
  *  getSegmentationDimensions
  *
  *****************************************/

  public String generateSegmentationDimensionID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSegmentationDimension(String segmentationDimensionID) { return getStoredGUIManagedObject(segmentationDimensionID); }
  public Collection<GUIManagedObject> getStoredSegmentationDimensions() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSegmentationDimension(GUIManagedObject segmentationDimensionUnchecked, Date date) { return isActiveGUIManagedObject(segmentationDimensionUnchecked, date); }
  public SegmentationDimension getActiveSegmentationDimension(String segmentationDimensionID, Date date) { return (SegmentationDimension) getActiveGUIManagedObject(segmentationDimensionID, date); }
  public Collection<SegmentationDimension> getActiveSegmentationDimensions(Date date) { return (Collection<SegmentationDimension>) getActiveGUIManagedObjects(date); }

  //
  //  getSegment
  //

  public Segment getSegment(String segmentID) { synchronized (this) { return segmentsByID.get(segmentID); } }

  /*****************************************
  *
  *  putSegmentationDimension
  *
  *****************************************/

  public void putSegmentationDimension(SegmentationDimension segmentationDimension, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID) throws GUIManagerException{
    
    this.uploadedFileService = uploadedFileService;
    this.subscriberIDService = subscriberIDService;
    
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    // validate dimension
    //
    
    segmentationDimension.validate();
    
    //
    //  put
    //

    putGUIManagedObject(segmentationDimension, now, newObject, userID);
    
    //
    // add to queue
    //
    
    if(segmentationDimension.getTargetingType().equals(SegmentationDimensionTargetingType.FILE)) {
      notifyListenerOfSegmentationDimension((SegmentationDimensionFileImport) segmentationDimension);
    }
  }

  /*****************************************
  *
  *  putIncompleteSegmentationDimension
  *
  *****************************************/

  public void putIncompleteSegmentationDimension(IncompleteObject segmentationDimension, boolean newObject, String userID)
  {
    putGUIManagedObject(segmentationDimension, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeSegmentationDimension
  *
  *****************************************/
  
  public void removeSegmentationDimension(String segmentationDimensionID, String userID) 
  { 
    removeGUIManagedObject(segmentationDimensionID, SystemTime.getCurrentTime(), userID); 
  }

  /*****************************************
  *
  *  notifyListener
  *
  *****************************************/

  private void notifyListenerOfSegmentationDimension(SegmentationDimensionFileImport segmentationDimension)
  {
    listenerQueue.add(segmentationDimension);
  }

  /*****************************************
  *
  *  runListener
  *
  *****************************************/
  
  private void runListener()
  {
    while (!stopRequested)
      {
        try
          {
            
            //
            //  now
            //            

            Date now = SystemTime.getCurrentTime();
            
            //
            //  get next 
            //

            SegmentationDimensionFileImport segmentationDimension = listenerQueue.take();
            
            /************************************************
            *
            *  open zookeeper and lock segmentationDimension
            *
            *************************************************/

            ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationDimension.getGUIManagedObjectID());
            
            //
            // time to work
            //
            
            if(segmentationDimension.getDimensionFileID() != null)
              {
                UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(segmentationDimension.getDimensionFileID());
                if (uploadedFile == null)
                  { 
                    log.warn("SegmentationDimensionService.run(uploaded file not found, processing done)");
                    return;
                  }
                else
                  {
                    //
                    //  parse file
                    //

                    BufferedReader reader;
                    try
                    {
                      AlternateID alternateID = Deployment.getAlternateIDs().get(uploadedFile.getCustomerAlternateID());
                      reader = new BufferedReader(new FileReader(UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename()));
                      for (String line; (line = reader.readLine()) != null && !line.isEmpty();)
                        {
                          String split[] = line.split(Deployment.getUploadedFileSeparator());
                          //Minimum two values (subscriberID;segment)
                          if(split.length >=2)
                            {   
                              String subscriberIDFromFile = split[0];
                              String segmentName = split[1];
                              String subscriberID = (alternateID != null) ? subscriberIDService.getSubscriberID(alternateID.getID(), subscriberIDFromFile) : subscriberIDFromFile;
                              if(subscriberID != null)
                                {
                                  if(segmentationDimension.getSegments() != null && !segmentationDimension.getSegments().isEmpty()) 
                                    {
                                      for(SegmentFileImport segment : segmentationDimension.getSegments()) 
                                        {
                                          if(segment.getName().equals(segmentName)) 
                                            {
                                              SubscriberGroup subscriberGroup = new SubscriberGroup(subscriberID, now, SubscriberGroupType.SegmentationDimension, Arrays.asList(segmentationDimension.getGUIManagedObjectID(), segment.getID()), segmentationDimension.getSubscriberGroupEpoch().getEpoch(), LoadType.Add.getAddRecord());
                                              kafkaProducer.send(new ProducerRecord<byte[], byte[]>(subscriberGroupTopic, stringKeySerde.serializer().serialize(subscriberGroupTopic, new StringKey(subscriberGroup.getSubscriberID())), subscriberGroupSerde.serializer().serialize(subscriberGroupTopic, subscriberGroup)));
                                            }
                                        }
                                    }
                                }
                              else
                                {
                                  log.warn("SegmentationDimensionService.run(cant resolve subscriberID ID="+line+")");
                                }
                            }
                        }
                      reader.close();
                    }
                    catch (IOException | SubscriberIDServiceException e)
                    {
                      log.warn("SegmentationDimensionService.run(problem with file parsing)", e);
                    }
                  }
              }

            /*****************************************
            *
            *  close
            *
            *****************************************/

            SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, segmentationDimension.getGUIManagedObjectID());
            subscriberIDService.close();
          }
        catch (InterruptedException e)
          {
            //
            // ignore
            //
          }
        catch (Throwable e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.warn("Exception processing listener: {}", stackTraceWriter.toString());
          }
      }
  }

  /*****************************************
  *
  *  interface SegmentationDimensionListener
  *
  *****************************************/

  public interface SegmentationDimensionListener
  {
    public void segmentationDimensionActivated(SegmentationDimension segmentationDimension);
    public void segmentationDimensionDeactivated(String guiManagedObjectID);
  }

}
