package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;

public class BadgeObjectiveService extends GUIService
{

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(BadgeObjectiveService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private BadgeObjectiveListener badgeObjectiveListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public BadgeObjectiveService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, BadgeObjectiveListener badgeObjectiveListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "BadgeObjectiveService", groupID, catalogObjectiveTopic, masterService, getSuperListener(badgeObjectiveListener), "putBadgeObjective", "removeBadgeObjective", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //
  
  public BadgeObjectiveService(String bootstrapServers, String groupID, String badgeObjectiveTopic, boolean masterService, BadgeObjectiveListener badgeObjectiveListener)
  {
    this(bootstrapServers, groupID, badgeObjectiveTopic, masterService, badgeObjectiveListener, true);
  }

  //
  //  constructor
  //

  public BadgeObjectiveService(String bootstrapServers, String groupID, String badgeObjectiveTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, badgeObjectiveTopic, masterService, (BadgeObjectiveListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(BadgeObjectiveListener badgeObjectiveListener)
  {
    GUIManagedObjectListener superListener = null;
    if (badgeObjectiveListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { badgeObjectiveListener.badgeObjectiveActivated((BadgeObjective) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { badgeObjectiveListener.badgeObjectiveDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getBadgeObjectives
  *
  *****************************************/

  public String generateBadgeObjectiveID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredBadgeObjective(String badgeObjectiveID) { return getStoredGUIManagedObject(badgeObjectiveID); }
  public GUIManagedObject getStoredBadgeObjective(String badgeObjectiveID, boolean includeArchived) { return getStoredGUIManagedObject(badgeObjectiveID, includeArchived); }
  public Collection<GUIManagedObject> getStoredBadgeObjectives(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredBadgeObjectives(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveBadgeObjective(GUIManagedObject badgeObjectiveUnchecked, Date date) { return isActiveGUIManagedObject(badgeObjectiveUnchecked, date); }
  public BadgeObjective getActiveBadgeObjective(String badgeObjectiveID, Date date) { return (BadgeObjective) getActiveGUIManagedObject(badgeObjectiveID, date); }
  public Collection<BadgeObjective> getActiveBadgeObjectives(Date date, int tenantID) { return (Collection<BadgeObjective>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putBadgeObjective
  *
  *****************************************/

  public void putBadgeObjective(GUIManagedObject badgeObjective, boolean newObject, String userID) { putGUIManagedObject(badgeObjective, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeBadgeObjective
  *
  *****************************************/

  public void removeBadgeObjective(String badgeObjectiveID, String userID, int tenantID) { removeGUIManagedObject(badgeObjectiveID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface BadgeObjectiveListener
  *
  *****************************************/

  public interface BadgeObjectiveListener
  {
    public void badgeObjectiveActivated(BadgeObjective badgeObjective);
    public void badgeObjectiveDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  badgeObjectiveListener
    //

    BadgeObjectiveListener badgeObjectiveListener = new BadgeObjectiveListener()
    {
      @Override public void badgeObjectiveActivated(BadgeObjective badgeObjective) { System.out.println("badgeObjective activated: " + badgeObjective.getBadgeObjectiveID()); }
      @Override public void badgeObjectiveDeactivated(String guiManagedObjectID) { System.out.println("badgeObjective deactivated: " + guiManagedObjectID); }
    };

    //
    //  badgeObjectiveService
    //

    BadgeObjectiveService badgeObjectiveService = new BadgeObjectiveService(Deployment.getBrokerServers(), "example-badgeObjectiveservice-001", Deployment.getBadgeObjectiveTopic(), false, badgeObjectiveListener);
    badgeObjectiveService.start();

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
