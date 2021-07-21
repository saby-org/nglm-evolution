/****************************************************************************
*
*  CommunicationChannelTimeWindowService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.Deployment;
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

  @Deprecated // groupID not used
  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, CommunicationChannelTimeWindowListener communicationChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CommunicationChannelTimeWindowService", groupID, catalogObjectiveTopic, masterService, getSuperListener(communicationChannelListener), "getCommunicationChannelTimeWindow", "putCommunicationChannelTimeWindow", notifyOnSignificantChange);
  }
  //
  //  constructor
  //

  @Deprecated // groupID not used
  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String communicationChannelTimeWindowTopic, boolean masterService, CommunicationChannelTimeWindowListener communicationChannelListener)
  {
    this(bootstrapServers, groupID, communicationChannelTimeWindowTopic, masterService, communicationChannelListener, true);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not used
  public CommunicationChannelTimeWindowService(String bootstrapServers, String groupID, String communicationChannelTimeWindowTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, communicationChannelTimeWindowTopic, masterService, (CommunicationChannelTimeWindowListener) null, true);
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
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { communicationChannelListener.communicationChannelTimeWindowActivated((CommunicationChannelTimeWindow) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { communicationChannelListener.communicationChannelTimeWindowDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getStoredCommunicationChannelTimeWindow
  *
  *****************************************/

  public String generateCommunicationChannelTimeWindowID(String communicationChannelID) { return "timewindow-" + communicationChannelID; }
  public String getCommunicationChannelTimeWindowID(String communicationChannelID) { return generateCommunicationChannelTimeWindowID(communicationChannelID);}
  public GUIManagedObject getStoredCommunicationChannelTimeWindow(String communicationChannelID) { return getStoredGUIManagedObject(getCommunicationChannelTimeWindowID(communicationChannelID)); }
  public GUIManagedObject getStoredCommunicationChannelTimeWindow(String communicationChannelID, boolean includeArchived) { return getStoredGUIManagedObject(getCommunicationChannelTimeWindowID(communicationChannelID), includeArchived); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelTimeWindows(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelTimeWindows(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveCommunicationChannelTimeWindow(GUIManagedObject timeWindowUnchecked, Date date) { return isActiveGUIManagedObject(timeWindowUnchecked, date); }
  public CommunicationChannelTimeWindow getActiveCommunicationChannelTimeWindow(String communicationChannelID, Date date) { return (CommunicationChannelTimeWindow) getActiveGUIManagedObject(getCommunicationChannelTimeWindowID(communicationChannelID), date); }
  public Collection<CommunicationChannelTimeWindow> getActiveCommunicationChannelTimeWindow(Date date, int tenantID) { return (Collection<CommunicationChannelTimeWindow>) getActiveGUIManagedObjects(date, tenantID); }

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

  public void removeCommunicationChannelTimeWindow(String communicationChannelID, String userID, int tenantID) { removeGUIManagedObject(getCommunicationChannelTimeWindowID(communicationChannelID), SystemTime.getCurrentTime(), userID, tenantID); }
  
  /*****************************************
  *
  *  interface CommunicationChannelTimeWindowListener
  *
  *****************************************/

  public interface CommunicationChannelTimeWindowListener
  {
    public void communicationChannelTimeWindowActivated(CommunicationChannelTimeWindow notificationDailyWindow);
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
      @Override public void communicationChannelTimeWindowActivated(CommunicationChannelTimeWindow notificationDailyWindow) { System.out.println("communicationChannelTimeWindowActivated activated: " + notificationDailyWindow.getGUIManagedObjectID()); }
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