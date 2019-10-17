/****************************************************************************
*
*  CommunicationChannelService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.NotificationDailyWindows.DailyWindow;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

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
  
  public CommunicationChannelService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, CommunicationChannelListener communicationChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CommunicationChannelService", groupID, catalogObjectiveTopic, masterService, getSuperListener(communicationChannelListener), "getCommunicationChannel", "putCommunicationChannel", notifyOnSignificantChange);
  }
  //
  //  constructor
  //
  
  public CommunicationChannelService(String bootstrapServers, String groupID, String communicationChannelTopic, boolean masterService, CommunicationChannelListener communicationChannelListener)
  {
    this(bootstrapServers, groupID, communicationChannelTopic, masterService, communicationChannelListener, true);
  }

  //
  //  constructor
  //

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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { communicationChannelListener.communicationChannelDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getContactPolicies
  *
  *****************************************/

  public String generateCommunicationChannelID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCommunicationChannel(String communicationChannelID) { return getStoredGUIManagedObject(communicationChannelID); }
  public GUIManagedObject getStoredCommunicationChannel(String communicationChannelID, boolean includeArchived) { return getStoredGUIManagedObject(communicationChannelID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCommunicationChannels() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredCommunicationChannels(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveCommunicationChannel(GUIManagedObject communicationChannelUnchecked, Date date) { return isActiveGUIManagedObject(communicationChannelUnchecked, date); }
  public CommunicationChannel getActiveCommunicationChannel(String communicationChannelID, Date date) { return (CommunicationChannel) getActiveGUIManagedObject(communicationChannelID, date); }
  public Collection<CommunicationChannel> getActiveContactPolicies(Date date) { return (Collection<CommunicationChannel>) getActiveGUIManagedObjects(date); }

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

  public void removeCommunicationChannel(String communicationChannelID, String userID) { removeGUIManagedObject(communicationChannelID, SystemTime.getCurrentTime(), userID); }
  
  /*****************************************
  *
  *  getEffectiveDeliveryTime
  *
  *****************************************/
  
  public Date getEffectiveDeliveryTime(String channelID, Date now)
  {
    Date effectiveDeliveryDate = now;
    CommunicationChannel communicationChannel = (CommunicationChannel) getActiveCommunicationChannel(channelID, now);
    if (communicationChannel != null && communicationChannel.getNotificationDailyWindows() != null)
      {
        effectiveDeliveryDate = NGLMRuntime.END_OF_TIME;
        Date today = RLMDateUtils.truncate(now, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
        for (int i=0; i<8; i++)
          {
            //
            //  check the i-th day
            //

            Date windowDay = RLMDateUtils.addDays(today, i, Deployment.getBaseTimeZone());
            Date nextDay = RLMDateUtils.addDays(today, i+1, Deployment.getBaseTimeZone());
            for (DailyWindow dailyWindow : communicationChannel.getTodaysDailyWindows(windowDay))
              {
                Date windowStartDate = dailyWindow.getFromDate(windowDay);
                Date windowEndDate = dailyWindow.getUntilDate(windowDay);
                if (EvolutionUtilities.isDateBetween(now, windowStartDate, windowEndDate))
                  {
                    effectiveDeliveryDate = now;
                  }
                else if (now.compareTo(windowStartDate) < 0)
                  {
                    effectiveDeliveryDate = windowStartDate.compareTo(effectiveDeliveryDate) < 0 ? windowStartDate : effectiveDeliveryDate;
                  }
              }

            //
            //  effectiveDeliveryDate found?
            //

            if (effectiveDeliveryDate.compareTo(nextDay) < 0)
              {
                break;
              }
          }
      } 
    return effectiveDeliveryDate;
  }

  /*****************************************
  *
  *  interface NotificationTimeWindowListener
  *
  *****************************************/

  public interface CommunicationChannelListener
  {
    public void communicationChannelActivated(CommunicationChannel communicationChannel);
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
    //  communicationChannelListener
    //

    CommunicationChannelListener communicationChannelListener = new CommunicationChannelListener()
    {
      @Override public void communicationChannelActivated(CommunicationChannel communicationChannel) { System.out.println("communicationChannel activated: " + communicationChannel.getGUIManagedObjectID()); }
      @Override public void communicationChannelDeactivated(String guiManagedObjectID) { System.out.println("communicationChannel deactivated: " + guiManagedObjectID); }
    };

    //
    //  communicationChannelService
    //

    CommunicationChannelService communicationChannelService = new CommunicationChannelService(Deployment.getBrokerServers(), "example-communicationChannelService-001", Deployment.getCommunicationChannelTopic(), false, communicationChannelListener);
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