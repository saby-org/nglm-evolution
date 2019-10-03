/****************************************************************************
*
*  CommunicationChannelBlackoutService.java
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

public class CommunicationChannelBlackoutService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CommunicationChannelBlackoutService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CommunicationChannelBlackoutListener communicationChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public CommunicationChannelBlackoutService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, CommunicationChannelBlackoutListener communicationChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CommunicationChannelBlackoutService", groupID, catalogObjectiveTopic, masterService, getSuperListener(communicationChannelListener), "getCommunicationChannelBlackout", "putCommunicationChannelBlackout", notifyOnSignificantChange);
  }
  //
  //  constructor
  //
  
  public CommunicationChannelBlackoutService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService, CommunicationChannelBlackoutListener communicationChannelListener)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, communicationChannelListener, true);
  }

  //
  //  constructor
  //

  public CommunicationChannelBlackoutService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, (CommunicationChannelBlackoutListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CommunicationChannelBlackoutListener communicationChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (communicationChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { communicationChannelListener.communicationChannelBlackoutActivated((CommunicationChannelBlackoutPeriod) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { communicationChannelListener.communicationChannelBlackoutDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getStoredCommunicationChannelBlackout
  *
  *****************************************/

  public String generateCommunicationChannelBlackoutID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCommunicationChannelBlackout(String blackoutPeriodID) { return getStoredGUIManagedObject(blackoutPeriodID); }
  public GUIManagedObject getStoredCommunicationChannelBlackout(String blackoutPeriodID, boolean includeArchived) { return getStoredGUIManagedObject(blackoutPeriodID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelBlackouts() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredCommunicationChannelBlackouts(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveCommunicationChannelBlackout(GUIManagedObject blackoutPeriodUnchecked, Date date) { return isActiveGUIManagedObject(blackoutPeriodUnchecked, date); }
  public CommunicationChannelBlackoutPeriod getActiveCommunicationChannelBlackout(String communicationChannelID, Date date) { return (CommunicationChannelBlackoutPeriod) getActiveGUIManagedObject(communicationChannelID, date); }
  public Collection<CommunicationChannelBlackoutPeriod> getActiveCommunicationChannelBlackout(Date date) { return (Collection<CommunicationChannelBlackoutPeriod>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putCommunicationChannelBlackout
  *
  *****************************************/

  public void putCommunicationChannelBlackout(GUIManagedObject communicationChannel, boolean newObject, String userID) throws GUIManagerException
  {

    //
    //  put
    //

    putGUIManagedObject(communicationChannel, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  putCommunicationChannelBlackout
  *
  *****************************************/

  public void putCommunicationChannelBlackout(IncompleteObject communicationChannel, boolean newObject, String userID)
  {
    try
      {
        putCommunicationChannelBlackout((GUIManagedObject) communicationChannel, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeCommunicationChannelBlackout
  *
  *****************************************/

  public void removeCommunicationChannelBlackout(String communicationChannelID, String userID) { removeGUIManagedObject(communicationChannelID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface CommunicationChannelBlackoutListener
  *
  *****************************************/

  public interface CommunicationChannelBlackoutListener
  {
    public void communicationChannelBlackoutActivated(CommunicationChannelBlackoutPeriod communicationChannel);
    public void communicationChannelBlackoutDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  communicationChannelBlackoutListener
    //

    CommunicationChannelBlackoutListener communicationChannelListener = new CommunicationChannelBlackoutListener()
    {
      @Override public void communicationChannelBlackoutActivated(CommunicationChannelBlackoutPeriod communicationChannel) { System.out.println("communicationChannelBlackout activated: " + communicationChannel.getGUIManagedObjectID()); }
      @Override public void communicationChannelBlackoutDeactivated(String guiManagedObjectID) { System.out.println("communicationChannelBlackout deactivated: " + guiManagedObjectID); }
    };

    //
    //  CommunicationChannelBlackoutService
    //

    CommunicationChannelBlackoutService communicationChannelService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "example-communicationChannelService-001", Deployment.getCommunicationChannelBlackoutTopic(), false, communicationChannelListener);
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