/****************************************************************************
*
*  PointService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { pointListener.pointDeactivated(guiManagedObjectID); }
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
  public GUIManagedObject getStoredPoint(String pointID) 
  {
    GUIManagedObject result = null;
    GUIManagedObject guiManagedObject = getStoredGUIManagedObject(pointID);
    if (guiManagedObject.getAccepted())
      {
        if (!((Point) guiManagedObject).isScoreType())
          {
            result = guiManagedObject;
          }
      }
    else
      {
        result = guiManagedObject;
      }
    return result;
  }
  public GUIManagedObject getStoredPoint(String pointID, boolean includeArchived) 
  {
    GUIManagedObject result = null;
    GUIManagedObject guiManagedObject = getStoredGUIManagedObject(pointID, includeArchived);
    if (guiManagedObject.getAccepted())
      {
        if (!((Point) guiManagedObject).isScoreType())
          {
            result = guiManagedObject;
          }
      }
    else
      {
        result = guiManagedObject;
      }
    return result;
  }
  public Collection<GUIManagedObject> getStoredPoints() 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects())
      {
        if (guiManagedObject.getAccepted())
          {
            if (!((Point) guiManagedObject).isScoreType())
              {
                result.add(guiManagedObject);
              }
          }
        else
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  public Collection<GUIManagedObject> getStoredPoints(boolean includeArchived) 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects(includeArchived))
      {
        if (guiManagedObject.getAccepted())
          {
            if (!((Point) guiManagedObject).isScoreType())
              {
                result.add(guiManagedObject);
              }
          }
        else
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  public boolean isActivePoint(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date) && !((Point) pointUnchecked).isScoreType(); }
  public Point getActivePoint(String pointID, Date date) 
  {
    Point result = (Point) getActiveGUIManagedObject(pointID, date);
    if (result.isScoreType()) return null;
    return result;
  }
  public Collection<Point> getActivePoints(Date date) 
  {
    Collection<Point> result = new HashSet<Point>();
    for (Point guiManagedObject : (Collection<Point>) getActiveGUIManagedObjects(date))
      {
        if (!guiManagedObject.isScoreType()) result.add(guiManagedObject);
      }

    return result;
  }
  
  /*****************************************
  *
  *  getScores
  *
  *****************************************/

  public GUIManagedObject getStoredScore(String scoreID) 
  {
    GUIManagedObject result = null;
    GUIManagedObject guiManagedObject = getStoredGUIManagedObject(scoreID);
    if (guiManagedObject.getAccepted() && ((Point) guiManagedObject).isScoreType())
      {
        result = guiManagedObject;
      }
    return result;
  }
  
  public GUIManagedObject getStoredScore(String scoreID, boolean includeArchived) 
  {
    GUIManagedObject result = null;
    GUIManagedObject guiManagedObject = getStoredGUIManagedObject(scoreID, includeArchived);
    if (guiManagedObject.getAccepted() && ((Point) guiManagedObject).isScoreType())
      {
        result = guiManagedObject;
      }
    return result;
  }
  public Collection<GUIManagedObject> getStoredScores() 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects())
      {
        if (guiManagedObject.getAccepted() && ((Point) guiManagedObject).isScoreType())
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  
  public Collection<GUIManagedObject> getStoredScores(boolean includeArchived) 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects(includeArchived))
      {
        if (guiManagedObject.getAccepted() && ((Point) guiManagedObject).isScoreType())
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  public boolean isActiveScore(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date) && ((Point) pointUnchecked).isScoreType(); }

  public Point getActiveScore(String pointID, Date date)
  {
    Point result = (Point) getActiveGUIManagedObject(pointID, date);
    if (!result.isScoreType()) return null;
    return result;
  }
  
  public Collection<Point> getActiveScores(Date date)
  {
    Collection<Point> result = new HashSet<Point>();
    for (Point guiManagedObject : (Collection<Point>) getActiveGUIManagedObjects(date))
      {
        if (guiManagedObject.isScoreType()) result.add(guiManagedObject);
      }

    return result;
  }

  /*****************************************
  *
  *  putPoint
  *
  *****************************************/

  public void putPoint(Point point, boolean newObject, String userID) throws GUIManagerException{
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(point, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putIncompleteOffer
  *
  *****************************************/

  public void putIncompletePoint(IncompleteObject offer, boolean newObject, String userID)
  {
    putGUIManagedObject(offer, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removePoint
  *
  *****************************************/

  public void removePoint(String pointID, String userID) { 
    removeGUIManagedObject(pointID, SystemTime.getCurrentTime(), userID);
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
