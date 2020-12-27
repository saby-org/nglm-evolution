package com.evolving.nglm.evolution;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.evolving.nglm.evolution.extracts.ExtractItem;
import com.evolving.nglm.evolution.extracts.ExtractManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.*;
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
import com.evolving.nglm.evolution.SubscriberGroup.SubscriberGroupType;
import com.evolving.nglm.evolution.SubscriberGroupLoader.LoadType;

public class TargetService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(TargetService.class);
  
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

  private TargetListener TargetListener = null;
  private volatile boolean stopRequested = false;
  private BlockingQueue<Target> listenerQueue = new LinkedBlockingQueue<Target>();
  private UploadedFileService uploadedFileService = null; 
  private SubscriberIDService subscriberIDService = null;
  private String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private Thread listenerThread = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService, TargetListener targetListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TargetService", groupID, targetTopic, masterService, getSuperListener(targetListener), "putTarget", "removeTarget", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public TargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService, TargetListener targetListener)
  {
    this(bootstrapServers, groupID, targetTopic, masterService, targetListener, true);
  }

  //
  //  constructor
  //

  public TargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, targetTopic, masterService, (TargetListener) null, true);
    
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
    listenerThread = new Thread(listener, "TargetService");
    listenerThread.start();
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(TargetListener targetListener)
  {
    GUIManagedObjectListener superListener = null;
    if (targetListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { targetListener.targetActivated((Target) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { targetListener.targetDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getTargets
  *
  *****************************************/

  public String generateTargetID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredTarget(String targetID, int tenantID) { return getStoredGUIManagedObject(targetID, tenantID); }
  public GUIManagedObject getStoredTarget(String targetID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(targetID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredTargets(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredTargets(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveTarget(GUIManagedObject targetUnchecked, Date date) { return isActiveGUIManagedObject(targetUnchecked, date); }
  public Target getActiveTarget(String targetID, Date date, int tenantID) { return (Target) getActiveGUIManagedObject(targetID, date, tenantID); }
  public Collection<Target> getActiveTargets(Date date, int tenantID) { return (Collection<Target>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(GUIManagedObject target, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID, int tenantID) throws GUIManagerException
  {
    this.uploadedFileService = uploadedFileService;
    this.subscriberIDService = subscriberIDService;
    
    //
    //  now
    //            

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (target instanceof Target)
      {
        ((Target) target).validate(uploadedFileService, now, tenantID);
      }
    
    //
    //  put
    //

    putGUIManagedObject(target, now, newObject, userID, tenantID);

    //
    //  process file
    //

    if (isActiveTarget(target, now))
      {
        notifyListenerOfTarget((Target) target);
      }
  }
  
  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(IncompleteObject target, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID, int tenantID)
  {
    try
      {
        putTarget((GUIManagedObject) target, uploadedFileService, subscriberIDService, newObject, userID, tenantID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeTarget
  *
  *****************************************/

  public void removeTarget(String targetID, String userID, int tenantID) { removeGUIManagedObject(targetID, SystemTime.getCurrentTime(), userID, tenantID); }
  
  /*****************************************
  *
  *  isTargetFileBeingProcessed
  *
  *****************************************/
  
  public boolean isTargetFileBeingProcessed (Target target) {
    if(listenerQueue != null) {
      if(listenerQueue.contains(target))
        return true;
    }
    return false;
  }

  /*****************************************
  *
  *  interface TargetListener
  *
  *****************************************/

  public interface TargetListener
  {
    public void targetActivated(Target target);
    public void targetDeactivated(String guiManagedObjectID);
  }
  
  /*****************************************
  *
  *  notifyListener
  *
  *****************************************/

  private void notifyListenerOfTarget(Target target)
  {
    listenerQueue.add(target);
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

            Target target = listenerQueue.take();
            
            /*****************************************
            *
            *  open zookeeper and lock target
            *
            *****************************************/

            ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(target.getTargetID());
            
            /*****************************************
            *
            *  retrieve existing epoch
            *
            *****************************************/

            SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, target.getTargetID());

            /*****************************************
            *
            *  submit new epoch (if necessary)
            *
            *****************************************/

            SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, target.getTargetID(), existingEpoch, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());
            
            //
            // time to work
            //
            
            if(target.getTargetFileID() != null)
              {
                UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(target.getTargetFileID(), target.getTenantID());
                if (uploadedFile == null)
                  { 
                    log.warn("TargetService.run(uploaded file not found, processing done)");
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
                        for (String line; (line = reader.readLine()) != null;)
                          {
                            if(line.trim().isEmpty())
                              {
                                if(log.isDebugEnabled()) log.debug("TargetService.run(skipping empty line)");
                                continue;
                              }
                            String subscriberID = (alternateID != null) ? subscriberIDService.getSubscriberID(alternateID.getID(), line) : line;
                            if(subscriberID != null)
                              {
                                SubscriberGroup subscriberGroup = new SubscriberGroup(subscriberID, now, SubscriberGroupType.Target, Arrays.asList(target.getTargetID()), subscriberGroupEpoch.getEpoch(), LoadType.Add.getAddRecord());
                                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(subscriberGroupTopic, stringKeySerde.serializer().serialize(subscriberGroupTopic, new StringKey(subscriberGroup.getSubscriberID())), subscriberGroupSerde.serializer().serialize(subscriberGroupTopic, subscriberGroup)));
                              }
                            else
                              {
                                log.warn("TargetService.run(cant resolve subscriberID ID="+line+")");
                              }
                          }
                        reader.close();
                      }
                    catch (IOException | SubscriberIDServiceException e)
                      {
                        log.warn("TargetService.run(problem with file parsing)", e);
                      }
                  }
              }
            
            /*****************************************
            *
            *  close
            *
            *****************************************/

            SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, target.getTargetID());
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
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  targetListener
    //

    TargetListener targetListener = new TargetListener()
    {
      @Override public void targetActivated(Target target) { System.out.println("Target activated: " + target.getGUIManagedObjectID()); }
      @Override public void targetDeactivated(String guiManagedObjectID) { System.out.println("Target deactivated: " + guiManagedObjectID); }
    };

    //
    //  targetService
    //

    TargetService targetService = new TargetService(Deployment.getBrokerServers(), "example-targetservice-001", Deployment.getTargetTopic(), false, targetListener);
    targetService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }
}