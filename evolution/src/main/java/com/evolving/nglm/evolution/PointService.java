/****************************************************************************
*
*  PointService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PointService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PointService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PointService(String bootstrapServers, String groupID, String pointTopic, boolean masterService, PointListener pointListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "pointService", groupID, pointTopic, masterService, getSuperListener(pointListener), "putPoint", "removePoint", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public PointService(String bootstrapServers, String groupID, String pointTopic, boolean masterService, PointListener pointListener)
  {
    this(bootstrapServers, groupID, pointTopic, masterService, pointListener, true);
  }

  //
  //  constructor
  //

  public PointService(String bootstrapServers, String groupID, String pointTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, pointTopic, masterService, (PointListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PointListener pointListener)
  {
    GUIManagedObjectListener superListener = null;
    if (pointListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { pointListener.pointActivated((Point) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { pointListener.pointDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
   *
   *  getSummaryJSONRepresentation
   *
   *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    return result;
  }

  /*****************************************
  *
  *  getPoints
  *
  *****************************************/

  public String generatePointID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPoint(String pointID, int tenantID) { return getStoredGUIManagedObject(pointID, tenantID); }
  public GUIManagedObject getStoredPoint(String pointID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(pointID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredPoints(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredPoints(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActivePoint(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date); }
  public Point getActivePoint(String pointID, Date date, int tenantID) { return (Point) getActiveGUIManagedObject(pointID, date, tenantID); }
  public Collection<Point> getActivePoints(Date date, int tenantID) { return (Collection<Point>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putPoint
  *
  *****************************************/

  public void putPoint(Point point, boolean newObject, String userID, int tenantID) throws GUIManagerException{
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(point, now, newObject, userID, tenantID);
  }
  
  /*****************************************
  *
  *  putIncompleteOffer
  *
  *****************************************/

  public void putIncompletePoint(IncompleteObject offer, boolean newObject, String userID, int tenantID)
  {
    putGUIManagedObject(offer, SystemTime.getCurrentTime(), newObject, userID, tenantID);
  }

  /*****************************************
  *
  *  removePoint
  *
  *****************************************/

  public void removePoint(String pointID, String userID, int tenantID) { 
    removeGUIManagedObject(pointID, SystemTime.getCurrentTime(), userID, tenantID);
  }

  /*****************************************
  *
  *  interface PointListener
  *
  *****************************************/

  public interface PointListener
  {
    public void pointActivated(Point point);
    public void pointDeactivated(String guiManagedObjectID);
  }

}
