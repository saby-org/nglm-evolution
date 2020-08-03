/****************************************************************************
*
*  CommunicationChannelTimeWindowService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class CommunicationChannelTimeWindowService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CommunicationChannelTimeWindowService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CommunicationChannelTimeWindowListener communicationChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, CommunicationChannelTimeWindowListener communicationChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CommunicationChannelTimeWindowService", groupID, catalogObjectiveTopic, masterService, getSuperListener(communicationChannelListener), "getCommunicationChannelTimeWindow", "putCommunicationChannelTimeWindow", notifyOnSignificantChange);
  }
  //
  //  constructor
  //
  
  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService, CommunicationChannelTimeWindowListener communicationChannelListener)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, communicationChannelListener, true);
  }

  //
  //  constructor
  //

  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, (CommunicationChannelTimeWindowListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CommunicationChannelTimeWindowListener communicationChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (communicationChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { communicationChannelListener.communicationChannelTimeWindowActivated((CommunicationChannelTimeWindows) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { communicationChannelListener.communicationChannelTimeWindowDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getStoredCommunicationChannelTimeWindow
  *
  *****************************************/

  public String generateCommunicationChannelTimeWindowID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCommunicationChannelTimeWindow(String blackoutPeriodID) { return getStoredGUIManagedObject(blackoutPeriodID); }
  public GUIManagedObject getStoredCommunicationChannelTimeWindow(String blackoutPeriodID, boolean includeArchived) { return getStoredGUIManagedObject(blackoutPeriodID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelTimeWindows() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelTimeWindows(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveCommunicationChannelTimeWindow(GUIManagedObject blackoutPeriodUnchecked, Date date) { return isActiveGUIManagedObject(blackoutPeriodUnchecked, date); }
  public CommunicationChannelTimeWindows getActiveCommunicationChannelTimeWindow(String communicationChannelID, Date date) { return (CommunicationChannelTimeWindows) getActiveGUIManagedObject(communicationChannelID, date); }
  public Collection<CommunicationChannelTimeWindows> getActiveCommunicationChannelTimeWindow(Date date) { return (Collection<CommunicationChannelTimeWindows>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putCommunicationChannelTimeWindow
  *
  *****************************************/

  public void putCommunicationChannelTimeWindow(GUIManagedObject communicationChannel, boolean newObject, String userID) throws GUIManagerException
  {

    //
    //  put
    //

    putGUIManagedObject(communicationChannel, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  putCommunicationChannelTimeWindow
  *
  *****************************************/

  public void putCommunicationChannelTimeWindow(IncompleteObject communicationChannel, boolean newObject, String userID)
  {
    try
      {
        putCommunicationChannelTimeWindow((GUIManagedObject) communicationChannel, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeCommunicationChannelTimeWindow
  *
  *****************************************/

  public void removeCommunicationChannelTimeWindow(String communicationChannelID, String userID) { removeGUIManagedObject(communicationChannelID, SystemTime.getCurrentTime(), userID); }
  
  /*****************************************
  *
  *  interface CommunicationChannelTimeWindowListener
  *
  *****************************************/

  public interface CommunicationChannelTimeWindowListener
  {
    public void communicationChannelTimeWindowActivated(CommunicationChannelTimeWindows notificationDailyWindow);
    public void communicationChannelTimeWindowDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  CommunicationChannelTimeWindowListener
    //

    CommunicationChannelTimeWindowListener timeWindowListener = new CommunicationChannelTimeWindowListener()
    {
      @Override public void communicationChannelTimeWindowActivated(CommunicationChannelTimeWindows notificationDailyWindow) { System.out.println("communicationChannelTimeWindowActivated activated: " + notificationDailyWindow.getGUIManagedObjectID()); }
      @Override public void communicationChannelTimeWindowDeactivated(String guiManagedObjectID) { System.out.println("communicationChannelTimeWindowActivated deactivated: " + guiManagedObjectID); }
    };

    //
    //  CommunicationChannelTimeWindowService
    //

    CommunicationChannelTimeWindowService communicationChannelTimeWindowService = new CommunicationChannelTimeWindowService(Deployment.getBrokerServers(), "examplecc-001", Deployment.getCommunicationChannelTimeWindowTopic(), false, timeWindowListener);
    communicationChannelTimeWindowService.start();

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