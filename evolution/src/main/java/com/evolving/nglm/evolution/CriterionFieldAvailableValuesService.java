/****************************************************************************
*
*  CriterionFieldAvailableValuesService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class CriterionFieldAvailableValuesService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriterionFieldAvailableValuesService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionFieldAvailableValuesListener criterionFieldAvailableValuesListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CriterionFieldAvailableValuesService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, CriterionFieldAvailableValuesListener criterionFieldAvailableValuesListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CriterionFieldAvailableValuesService", groupID, journeyTopic, masterService, getSuperListener(criterionFieldAvailableValuesListener), "putCriterionFieldAvailableValues", "removeCriterionFieldAvailableValues", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public CriterionFieldAvailableValuesService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, CriterionFieldAvailableValuesListener criterionFieldAvailableValuesListener)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, criterionFieldAvailableValuesListener, true);
  }

  //
  //  constructor
  //

  public CriterionFieldAvailableValuesService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, (CriterionFieldAvailableValuesListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CriterionFieldAvailableValuesListener criterionFieldAvailableValuesListener)
  {
    GUIManagedObjectListener superListener = null;
    if (criterionFieldAvailableValuesListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { criterionFieldAvailableValuesListener.criterionFieldAvailableValuesActivated((CriterionFieldAvailableValues) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { criterionFieldAvailableValuesListener.criterionFieldAvailableValuesDeactivated(guiManagedObjectID);}
        };
      }
    return superListener;
  }
  
  /*****************************************
  *
  *  getJourneys
  *
  *****************************************/

  public String generateCriterionFieldAvailableValuesID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCriterionFieldAvailableValues(String criterionFieldAvailableValuesID) { return getStoredGUIManagedObject(criterionFieldAvailableValuesID); }
  public GUIManagedObject getStoredCriterionFieldAvailableValues(String criterionFieldAvailableValuesID, boolean includeArchived) { return getStoredGUIManagedObject(criterionFieldAvailableValuesID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCriterionFieldAvailableValuesList(int tenantId) { return getStoredGUIManagedObjects(tenantId); }
  public Collection<GUIManagedObject> getStoredCriterionFieldAvailableValuesList(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveCriterionFieldAvailableValues(GUIManagedObject criterionFieldAvailableValuesUnchecked, Date date) { return isActiveGUIManagedObject(criterionFieldAvailableValuesUnchecked, date); }
  public Journey getActiveCriterionFieldAvailableValues(String journeyID, Date date) { return (Journey) getActiveGUIManagedObject(journeyID, date); }
  public Collection<CriterionFieldAvailableValues> getActiveCriterionFieldAvailableValues(Date date, int tenantID) { return (Collection<CriterionFieldAvailableValues>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putCriterionFieldAvailableValues
  *
  *****************************************/

  public void putCriterionFieldAvailableValues(GUIManagedObject criterionFieldAvailableValues, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(criterionFieldAvailableValues, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putCriterionFieldAvailableValues
  *
  *****************************************/

  public void putCriterionFieldAvailableValues(IncompleteObject criterionFieldAvailableValues, boolean newObject, String userID)
  {
    try
      {
        putCriterionFieldAvailableValues((GUIManagedObject) criterionFieldAvailableValues, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeCriterionFieldAvailableValues
  *
  *****************************************/

  public void removeCriterionFieldAvailableValues(String criterionFieldAvailableValuesID, String userID, int tenantID) { removeGUIManagedObject(criterionFieldAvailableValuesID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface CriterionFieldAvailableValuesListener
  *
  *****************************************/

  public interface CriterionFieldAvailableValuesListener
  {
    public void criterionFieldAvailableValuesActivated(CriterionFieldAvailableValues criterionFieldAvailableValues);
    public void criterionFieldAvailableValuesDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  journeyListener
    //

    CriterionFieldAvailableValuesListener criterionFieldAvailableValuesListener = new CriterionFieldAvailableValuesListener()
    {
      @Override public void criterionFieldAvailableValuesActivated(CriterionFieldAvailableValues criterionFieldAvailableValues) { System.out.println("criterionFieldAvailableValues activated: " + criterionFieldAvailableValues.getGUIManagedObjectID()); }
      @Override public void criterionFieldAvailableValuesDeactivated(String guiManagedObjectID) { System.out.println("criterionFieldAvailableValues deactivated: " + guiManagedObjectID); }
    };

    //
    //  criterionFieldAvailableValuesService
    //

    CriterionFieldAvailableValuesService criterionFieldAvailableValuesService = new CriterionFieldAvailableValuesService(Deployment.getBrokerServers(), "example-criterionFieldAvailableValuesService-001", Deployment.getJourneyTopic(), false, criterionFieldAvailableValuesListener);
    criterionFieldAvailableValuesService.start();

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
