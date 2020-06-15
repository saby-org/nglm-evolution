package com.evolving.nglm.evolution;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.evolving.nglm.evolution.extracts.ExtractItem;
import com.evolving.nglm.evolution.extracts.ExtractManager;
import com.evolving.nglm.evolution.reports.ReportManager;
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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { targetListener.targetDeactivated(guiManagedObjectID); }
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
  public GUIManagedObject getStoredTarget(String targetID) { return getStoredGUIManagedObject(targetID); }
  public GUIManagedObject getStoredTarget(String targetID, boolean includeArchived) { return getStoredGUIManagedObject(targetID, includeArchived); }
  public Collection<GUIManagedObject> getStoredTargets() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredTargets(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveTarget(GUIManagedObject targetUnchecked, Date date) { return isActiveGUIManagedObject(targetUnchecked, date); }
  public Target getActiveTarget(String targetID, Date date) { return (Target) getActiveGUIManagedObject(targetID, date); }
  public Collection<Target> getActiveTargets(Date date) { return (Collection<Target>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(GUIManagedObject target, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID) throws GUIManagerException
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
        ((Target) target).validate(uploadedFileService, now);
      }
    
    //
    //  put
    //

    putGUIManagedObject(target, now, newObject, userID);

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

  public void putTarget(IncompleteObject target, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID)
  {
    try
      {
        putTarget((GUIManagedObject) target, uploadedFileService, subscriberIDService, newObject, userID);
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

  public void removeTarget(String targetID, String userID) { removeGUIManagedObject(targetID, SystemTime.getCurrentTime(), userID); }
  
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
                UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(target.getTargetFileID());
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
                        for (String line; (line = reader.readLine()) != null && !line.isEmpty();)
                          {
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

  private ZooKeeper zk = null;
  private static final int NB_TIMES_TO_TRY = 10;

  /*
   * Called by GuiManager to trigger a one-time report
   */
  public void launchGenerateAndDownloadExtract(ExtractItem extractItem)
  {
    log.trace("launchTarget : " + extractItem.getExtractFileName());
    String znode = ExtractManager.getControlDir()+ File.separator + "launchTarget-" + extractItem.getExtractFileName()+"-"+extractItem.getUserId() + "-";
    if (getZKConnection())
      {
        log.debug("Trying to create ephemeral znode " + znode + " for " + extractItem.getExtractFileName());
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

  private boolean isConnectionValid(ZooKeeper zookeeper)
  {
    return (zookeeper != null) && (zookeeper.getState() == ZooKeeper.States.CONNECTED);
  }

  public boolean isTargetExtractRunning(String targetName) {
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

  private boolean getZKConnection()  {
    if (!isConnectionValid(zk)) {
      log.debug("Trying to acquire ZooKeeper connection to "+Deployment.getZookeeperConnect());
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