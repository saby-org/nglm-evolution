package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TargetListener TargetListener = null;

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
  public Collection<GUIManagedObject> getStoredTargets() { return getStoredGUIManagedObjects(); }
  public boolean isActiveTarget(GUIManagedObject targetUnchecked, Date date) { return isActiveGUIManagedObject(targetUnchecked, date); }
  public Target getActiveTarget(String targetID, Date date) { return (Target) getActiveGUIManagedObject(targetID, date); }
  public Collection<Target> getActiveTargets(Date date) { return (Collection<Target>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(GUIManagedObject target, UploadedFileService uploadedFileService, boolean newObject, String userID) throws GUIManagerException
  {
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
  }
  
  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(IncompleteObject target, UploadedFileService uploadedFileService, boolean newObject, String userID)
  {
    try
      {
        putTarget((GUIManagedObject) target, uploadedFileService, newObject, userID);
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