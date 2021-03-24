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
    if (!(getStoredGUIManagedObject(pointID) instanceof Score))
      {
        result = getStoredGUIManagedObject(pointID); 
      }
    return result;
  }
  public GUIManagedObject getStoredPoint(String pointID, boolean includeArchived) 
  {
    GUIManagedObject result = null;
    if (!(getStoredGUIManagedObject(pointID, includeArchived) instanceof Score))
      {
        result = getStoredGUIManagedObject(pointID, includeArchived); 
      }
    return result;
  }
  public Collection<GUIManagedObject> getStoredPoints() 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects())
      {
        if (!(guiManagedObject instanceof Score))
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
        if (!(guiManagedObject instanceof Score))
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  public boolean isActivePoint(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date) && !(pointUnchecked instanceof Score); }
  public Point getActivePoint(String pointID, Date date) 
  {
    Point result = null;
    if (!(getActiveGUIManagedObject(pointID, date) instanceof Score))
      {
        result = (Point) getActiveGUIManagedObject(pointID, date);
      }
    return result;
  }
  public Collection<Point> getActivePoints(Date date) 
  {
    Collection<Point> result = new HashSet<Point>();
    for (GUIManagedObject guiManagedObject : getActiveGUIManagedObjects(date))
      {
        if (!(guiManagedObject instanceof Score)) result.add((Point) guiManagedObject);
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
    if ((getStoredGUIManagedObject(scoreID) instanceof Score))
      {
        result = getStoredGUIManagedObject(scoreID); 
      }
    return result;
  }
  public GUIManagedObject getStoredScore(String scoreID, boolean includeArchived) 
  {
    GUIManagedObject result = null;
    if ((getStoredGUIManagedObject(scoreID, includeArchived) instanceof Score))
      {
        result = getStoredGUIManagedObject(scoreID, includeArchived); 
      }
    return result;
  }
  public Collection<GUIManagedObject> getStoredScores() 
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : getStoredGUIManagedObjects())
      {
        if (guiManagedObject instanceof Score)
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
        if ((guiManagedObject instanceof Score))
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }
  public boolean isActiveScore(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date) && (pointUnchecked instanceof Score); }

  public Score getActiveScore(String pointID, Date date)
  {
    Score result = null;
    if (getActiveGUIManagedObject(pointID, date) instanceof Score)
      {
        result = (Score) getActiveGUIManagedObject(pointID, date);
      }
    return result;
  }
  
  public Collection<Score> getActiveScores(Date date)
  {
    Collection<Score> result = new HashSet<Score>();
    for (GUIManagedObject guiManagedObject : getActiveGUIManagedObjects(date))
      {
        if (guiManagedObject instanceof Score)
          result.add((Score) guiManagedObject);
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
