package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class UploadedTargetService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UploadedTargetService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private UploadedTargetListener TargetListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UploadedTargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService, UploadedTargetListener targetListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "UploadedTargetService", groupID, targetTopic, masterService, getSuperListener(targetListener), "putUploadedTarget", "removeUploadedTarget", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public UploadedTargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService, UploadedTargetListener targetListener)
  {
    this(bootstrapServers, groupID, targetTopic, masterService, targetListener, true);
  }

  //
  //  constructor
  //

  public UploadedTargetService(String bootstrapServers, String groupID, String targetTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, targetTopic, masterService, (UploadedTargetListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(UploadedTargetListener targetListener)
  {
    GUIManagedObjectListener superListener = null;
    if (targetListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { targetListener.targetActivated((UploadedTarget) guiManagedObject); }
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
  public UploadedTarget getActiveTarget(String targetID, Date date) { return (UploadedTarget) getActiveGUIManagedObject(targetID, date); }

  /*****************************************
  *
  *  putTarget
  *
  *****************************************/

  public void putTarget(GUIManagedObject target, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (target instanceof UploadedTarget)
      {
        
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

  public void putTarget(IncompleteObject target, boolean newObject, String userID)
  {
    try
      {
        putTarget((GUIManagedObject) target, newObject, userID);
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

  public interface UploadedTargetListener
  {
    public void targetActivated(UploadedTarget target);
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

    UploadedTargetListener targetListener = new UploadedTargetListener()
    {
      @Override public void targetActivated(UploadedTarget target) { System.out.println("Target activated: " + target.getGUIManagedObjectID()); }
      @Override public void targetDeactivated(String guiManagedObjectID) { System.out.println("Target deactivated: " + guiManagedObjectID); }
    };

    //
    //  targetService
    //

    UploadedTargetService targetService = new UploadedTargetService(Deployment.getBrokerServers(), "example-targetservice-001", Deployment.getTargetTopic(), false, targetListener);
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