/****************************************************************************
*
*  PointService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

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
  public GUIManagedObject getStoredPoint(String pointID) { return getStoredGUIManagedObject(pointID); }
  public GUIManagedObject getStoredPoint(String pointID, boolean includeArchived) { return getStoredGUIManagedObject(pointID, includeArchived); }
  public Collection<GUIManagedObject> getStoredPoints() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredPoints(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActivePoint(GUIManagedObject pointUnchecked, Date date) { return isActiveGUIManagedObject(pointUnchecked, date); }
  public Point getActivePoint(String pointID, Date date) { return (Point) getActiveGUIManagedObject(pointID, date); }
  public Collection<Point> getActivePoints(Date date) { return (Collection<Point>) getActiveGUIManagedObjects(date); }

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
    
    //
    // Create associated criterion
    //
    // point.<name>.balance
    {
      String pointName = point.getPointName();
      String id = computeCriterionId(pointName);
      JSONObject criterionFieldJSON = new JSONObject();
      criterionFieldJSON.put("id", id);
      criterionFieldJSON.put("display", "Point "+pointName+" Balance");
      criterionFieldJSON.put("dataType", "integer");
      criterionFieldJSON.put("tagFormat", null);
      criterionFieldJSON.put("tagMaxLength", null);
      criterionFieldJSON.put("esField", null);
      criterionFieldJSON.put("retriever", null);
      criterionFieldJSON.put("minValue", 0);
      criterionFieldJSON.put("maxValue", null);
      criterionFieldJSON.put("includedOperators", null);
      JSONArray excludedOperatorsField = new JSONArray(); excludedOperatorsField.add(".*null.*"); criterionFieldJSON.put("excludedOperators", excludedOperatorsField);
      criterionFieldJSON.put("includedComparableFields", null);
      JSONArray excludedComparableFields = new JSONArray(); excludedComparableFields.add(".*ID"); criterionFieldJSON.put("excludedComparableFields", excludedComparableFields);
      criterionFieldJSON.put("availableValues", null);
      CriterionField criterionField = new CriterionField(criterionFieldJSON);
      LoyaltyProgramService.putLoyaltyCriterionFields(criterionField.getID(), criterionField);
    }

  }

  /*****************************************
  *
  *  computeCriterionId
  *
  *****************************************/

  private static String computeCriterionId(String pointName)
  {
    return "point."+pointName+".balance";
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

    Point point = (Point) getStoredPoint(pointID);
    if (point != null)
      {
        LoyaltyProgramService.removeLoyaltyCriterionFields(computeCriterionId(point.getPointName()));
      }

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
