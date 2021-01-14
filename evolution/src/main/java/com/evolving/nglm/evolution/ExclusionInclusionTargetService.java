package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;
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

public class ExclusionInclusionTargetService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ExclusionInclusionTargetService.class);
  
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

  private ExclusionInclusionTargetListener exclusionInclusionTargetListener = null;
  private volatile boolean stopRequested = false;
  private BlockingQueue<ExclusionInclusionTarget> listenerQueue = new LinkedBlockingQueue<ExclusionInclusionTarget>();
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

  public ExclusionInclusionTargetService(String bootstrapServers, String groupID, String exclusionInclusionTargetTopic, boolean masterService, ExclusionInclusionTargetListener exclusionInclusionTargetListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TargetService", groupID, exclusionInclusionTargetTopic, masterService, getSuperListener(exclusionInclusionTargetListener), "putExclusionInclusionTarget", "removeExclusionInclusionTarget", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public ExclusionInclusionTargetService(String bootstrapServers, String groupID, String exclusionInclusionTargetTopic, boolean masterService, ExclusionInclusionTargetListener exclusionInclusionTargetListener)
  {
    this(bootstrapServers, groupID, exclusionInclusionTargetTopic, masterService, exclusionInclusionTargetListener, true);
  }

  //
  //  constructor
  //

  public ExclusionInclusionTargetService(String bootstrapServers, String groupID, String exclusionInclusionTargetTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, exclusionInclusionTargetTopic, masterService, (ExclusionInclusionTargetListener) null, true);
    
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
    listenerThread = new Thread(listener, "ExclusionInclusionTargetService");
    listenerThread.start();
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ExclusionInclusionTargetListener exclusionInclusionTargetListener)
  {
    GUIManagedObjectListener superListener = null;
    if (exclusionInclusionTargetListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { exclusionInclusionTargetListener.exclusionInclusionTargetActivated((ExclusionInclusionTarget) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { exclusionInclusionTargetListener.exclusionInclusionTargetDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getTargets
  *
  *****************************************/

  public String generateExclusionInclusionTargetID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredExclusionInclusionTarget(String exclusionInclusionTargetID) { return getStoredGUIManagedObject(exclusionInclusionTargetID); }
  public GUIManagedObject getStoredExclusionInclusionTarget(String exclusionInclusionTargetID, boolean includeArchived) { return getStoredGUIManagedObject(exclusionInclusionTargetID, includeArchived); }
  public Collection<GUIManagedObject> getStoredExclusionInclusionTargets(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredExclusionInclusionTargets(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveExclusionInclusionTarget(GUIManagedObject exclusionInclusionTargetUnchecked, Date date) { return isActiveGUIManagedObject(exclusionInclusionTargetUnchecked, date); }
  public ExclusionInclusionTarget getActiveExclusionInclusionTarget(String exclusionInclusionTargetID, Date date) { return (ExclusionInclusionTarget) getActiveGUIManagedObject(exclusionInclusionTargetID, date); }
  public Collection<ExclusionInclusionTarget> getActiveExclusionInclusionTargets(Date date, int tenantID) { return (Collection<ExclusionInclusionTarget>) getActiveGUIManagedObjects(date, tenantID); }

  //
  //  getActiveInclusionTargets
  //

  public Collection<ExclusionInclusionTarget> getActiveInclusionTargets(Date date, int tenantID)
  {
    Set<ExclusionInclusionTarget> result = new HashSet<ExclusionInclusionTarget>();
    for (ExclusionInclusionTarget exclusionInclusionTarget : getActiveExclusionInclusionTargets(date, tenantID))
      {
        switch (exclusionInclusionTarget.getTargetType())
          {
            case Inclusion:
              result.add(exclusionInclusionTarget);
              break;
          }
      }
    return result;
  }

  //
  //  getActiveExclusionTargets
  //

  public Collection<ExclusionInclusionTarget> getActiveExclusionTargets(Date date, int tenantID)
  {
    Set<ExclusionInclusionTarget> result = new HashSet<ExclusionInclusionTarget>();
    for (ExclusionInclusionTarget exclusionInclusionTarget : getActiveExclusionInclusionTargets(date, tenantID))
      {
        switch (exclusionInclusionTarget.getTargetType())
          {
            case Exclusion:
              result.add(exclusionInclusionTarget);
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putExclusionInclusionTarget(GUIManagedObject exclusionInclusionTarget, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID) throws GUIManagerException
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

    if (exclusionInclusionTarget instanceof ExclusionInclusionTarget)
      {
        ((ExclusionInclusionTarget) exclusionInclusionTarget).validate(uploadedFileService, now);
      }
    
    //
    //  put
    //

    putGUIManagedObject(exclusionInclusionTarget, now, newObject, userID);

    //
    //  process file
    //

    if (isActiveExclusionInclusionTarget(exclusionInclusionTarget, now))
      {
        notifyListenerOfExclusionInclusionTarget((ExclusionInclusionTarget) exclusionInclusionTarget);
      }
  }
  
  /*****************************************
  *
  *  putExclusionInclusionTarget
  *
  *****************************************/

  public void putExclusionInclusionTarget(IncompleteObject exclusionInclusionTarget, UploadedFileService uploadedFileService, SubscriberIDService subscriberIDService, boolean newObject, String userID)
  {
    try
      {
        putExclusionInclusionTarget((GUIManagedObject) exclusionInclusionTarget, uploadedFileService, subscriberIDService, newObject, userID);
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

  public void removeExclusionInclusionTarget(String exclusionInclusionTargetID, String userID, int tenantID) { removeGUIManagedObject(exclusionInclusionTargetID, SystemTime.getCurrentTime(), userID, tenantID); }
  
  /*****************************************
  *
  *  isTargetFileBeingProcessed
  *
  *****************************************/
  
  public boolean isExclusionInclusionTargetFileBeingProcessed (ExclusionInclusionTarget exclusionInclusionTarget) {
    if(listenerQueue != null) {
      if(listenerQueue.contains(exclusionInclusionTarget))
        return true;
    }
    return false;
  }

  /*****************************************
  *
  *  interface ExclusionInclusionTargetListener
  *
  *****************************************/

  public interface ExclusionInclusionTargetListener
  {
    public void exclusionInclusionTargetActivated(ExclusionInclusionTarget exclusionInclusionTarget);
    public void exclusionInclusionTargetDeactivated(String guiManagedObjectID);
  }
  
  /*****************************************
  *
  *  notifyListener
  *
  *****************************************/

  private void notifyListenerOfExclusionInclusionTarget(ExclusionInclusionTarget exclusionInclusionTarget)
  {
    listenerQueue.add(exclusionInclusionTarget);
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

            ExclusionInclusionTarget exclusionInclusionTarget = listenerQueue.take();
            
            /*****************************************
            *
            *  open zookeeper and lock exclusionInclusionTarget
            *
            *****************************************/

            ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(exclusionInclusionTarget.getExclusionInclusionTargetID());
            
            /*****************************************
            *
            *  retrieve existing epoch
            *
            *****************************************/

            SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, exclusionInclusionTarget.getExclusionInclusionTargetID());

            /*****************************************
            *
            *  submit new epoch (if necessary)
            *
            *****************************************/

            SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, exclusionInclusionTarget.getExclusionInclusionTargetID(), existingEpoch, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());
            
            //
            // time to work
            //
            
            if(exclusionInclusionTarget.getFileID() != null)
              {
                UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(exclusionInclusionTarget.getFileID());
                if (uploadedFile == null)
                  { 
                    log.warn("ExclusionInclusionTargetService.run(uploaded file not found, processing done)");
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
                                SubscriberGroup subscriberGroup = new SubscriberGroup(subscriberID, now, SubscriberGroupType.ExclusionInclusionTarget, Arrays.asList(exclusionInclusionTarget.getExclusionInclusionTargetID()), subscriberGroupEpoch.getEpoch(), LoadType.Add.getAddRecord(), exclusionInclusionTarget.getTenantID());
                                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(subscriberGroupTopic, stringKeySerde.serializer().serialize(subscriberGroupTopic, new StringKey(subscriberGroup.getSubscriberID())), subscriberGroupSerde.serializer().serialize(subscriberGroupTopic, subscriberGroup)));
                              }
                            else
                              {
                                log.warn("ExclusionInclusionTargetService.run(cant resolve subscriberID ID="+line+")");
                              }
                          }
                        reader.close();
                      }
                    catch (IOException | SubscriberIDServiceException e)
                      {
                        log.warn("ExclusionInclusionTargetService.run(problem with file parsing)", e);
                      }
                  }
              }
            
            /*****************************************
            *
            *  close
            *
            *****************************************/

            SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, exclusionInclusionTarget.getExclusionInclusionTargetID());
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
    //  exclusionInclusionTargetListener
    //

    ExclusionInclusionTargetListener exclusionInclusionTargetListener = new ExclusionInclusionTargetListener()
    {
      @Override public void exclusionInclusionTargetActivated(ExclusionInclusionTarget exclusionInclusionTarget) { System.out.println("ExclusionInclusionTarget activated: " + exclusionInclusionTarget.getGUIManagedObjectID()); }
      @Override public void exclusionInclusionTargetDeactivated(String guiManagedObjectID) { System.out.println("ExclusionInclusionTarget deactivated: " + guiManagedObjectID); }
    };

    //
    //  exclusionInclusionTargetService
    //

    ExclusionInclusionTargetService exclusionInclusionTargetService = new ExclusionInclusionTargetService(Deployment.getBrokerServers(), "example-exclusionInclusionTargetservice-001", Deployment.getExclusionInclusionTargetTopic(), false, exclusionInclusionTargetListener);
    exclusionInclusionTargetService.start();

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