/****************************************************************************
*
*  CommunicationChannelService.java
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

public class CommunicationChannelService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CommunicationChannelService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CommunicationChannelListener communicationChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  @Deprecated // groupID not used
  public CommunicationChannelService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService, CommunicationChannelListener communicationChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CommunicationChannelService", groupID, communicationChannelTopic, masterService, getSuperListener(communicationChannelListener), "getCommunicationChannel", "putCommunicationChannel", notifyOnSignificantChange);
  }
  //
  //  constructor
  //

  @Deprecated // groupID not used
  public CommunicationChannelService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService, CommunicationChannelListener communicationChannelListener)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, communicationChannelListener, true);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not used
  public CommunicationChannelService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, (CommunicationChannelListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CommunicationChannelListener communicationChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (communicationChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { communicationChannelListener.communicationChannelActivated((CommunicationChannel) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { communicationChannelListener.communicationChannelDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getStoredCommunicationChannel
  *
  *****************************************/

  public String generateCommunicationChannelID(String communicationChannelID) { return communicationChannelID; }
  public String getCommunicationChannelID(String communicationChannelID) { return generateCommunicationChannelID(communicationChannelID);}
  public GUIManagedObject getStoredCommunicationChannel(String communicationChannelID) { return getStoredGUIManagedObject(getCommunicationChannelID(communicationChannelID)); }
  public GUIManagedObject getStoredCommunicationChannel(String communicationChannelID, boolean includeArchived) { return getStoredGUIManagedObject(getCommunicationChannelID(communicationChannelID), includeArchived); }
  public Collection<GUIManagedObject> getStoredCommunicationChannels(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredCommunicationChannels(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveCommunicationChannel(GUIManagedObject communicationChannelUnchecked, Date date) { return isActiveGUIManagedObject(communicationChannelUnchecked, date); }
  public CommunicationChannel getActiveCommunicationChannel(String communicationChannelID, Date date) { return (CommunicationChannel) getActiveGUIManagedObject(getCommunicationChannelID(communicationChannelID), date); }
  public Collection<CommunicationChannel> getActiveCommunicationChannel(Date date, int tenantID) { return (Collection<CommunicationChannel>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putCommunicationChannel
  *
  *****************************************/

  public void putCommunicationChannel(GUIManagedObject communicationChannel, boolean newObject, String userID) throws GUIManagerException
  {

    //
    //  put
    //

    putGUIManagedObject(communicationChannel, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  putCommunicationChannel
  *
  *****************************************/

  public void putCommunicationChannel(IncompleteObject communicationChannel, boolean newObject, String userID)
  {
    try
      {
        putCommunicationChannel((GUIManagedObject) communicationChannel, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeCommunicationChannel
  *
  *****************************************/

  public void removeCommunicationChannel(String communicationChannelID, String userID, int tenantID) { removeGUIManagedObject(getCommunicationChannelID(communicationChannelID), SystemTime.getCurrentTime(), userID, tenantID); }
  
  /*****************************************
  *
  *  interface CommunicationChannelListener
  *
  *****************************************/

  public interface CommunicationChannelListener
  {
    public void communicationChannelActivated(CommunicationChannel notificationDailyWindow);
    public void communicationChannelDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  CommunicationChannelListener
    //

    CommunicationChannelListener communicationChannelListener = new CommunicationChannelListener()
    {
      @Override public void communicationChannelActivated(CommunicationChannel notificationDailyWindow) { System.out.println("communicationChannelActivated activated: " + notificationDailyWindow.getGUIManagedObjectID()); }
      @Override public void communicationChannelDeactivated(String guiManagedObjectID) { System.out.println("communicationChannelActivated deactivated: " + guiManagedObjectID); }
    };

    //
    //  CommunicationChannelService
    //

    CommunicationChannelService communicationChannelService = new CommunicationChannelService(Deployment.getBrokerServers(), "examplecc-001", Deployment.getCommunicationChannelTopic(), false, communicationChannelListener);
    communicationChannelService.start();

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