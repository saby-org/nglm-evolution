/****************************************************************************
*
*  SegmentationDimensionService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService;

public class SegmentationDimensionService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentationDimensionService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String,Segment> segmentsByID = new HashMap<String,Segment>();

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService, SegmentationDimensionListener segmentationDimensionListener, boolean notifyOnSignificantChange)
  {
    //
    //  super
    //  

    super(bootstrapServers, "segmentationDimensionService", groupID, segmentationDimensionTopic, masterService, getSuperListener(segmentationDimensionListener), "putSegmentationDimension", "removeSegmentationDimension", notifyOnSignificantChange);

    //
    //  listener
    //

    GUIManagedObjectListener segmentListener = new GUIManagedObjectListener()
    {
      //
      //  guiManagedObjectActivated
      //

      @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject)
      {
        SegmentationDimension segmentationDimension = (SegmentationDimension) guiManagedObject;
        for (Segment segment : segmentationDimension.getSegments())
          {
            segmentsByID.put(segment.getID(), segment);
          }
      }

      //
      //  guiManagedObjectDeactivated
      //

      @Override public void guiManagedObjectDeactivated(String guiManagedObjectID)
      {
        //
        //  ignore
        //
      }
    };

    //
    //  register
    //

    registerListener(segmentListener);

    //
    //  initialize segmentsByID
    //

    Date now = SystemTime.getCurrentTime();
    for (SegmentationDimension segmentationDimension : getActiveSegmentationDimensions(now))
      {
        for (Segment segment : segmentationDimension.getSegments())
          {
            segmentsByID.put(segment.getID(), segment);
          }
      }
  }

  //
  //  constructor
  //

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService, SegmentationDimensionListener segmentationDimensionListener)
  {
    this(bootstrapServers, groupID, segmentationDimensionTopic, masterService, segmentationDimensionListener, true);
  }

  //
  //  constructor
  //

  public SegmentationDimensionService(String bootstrapServers, String groupID, String segmentationDimensionTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, segmentationDimensionTopic, masterService, (SegmentationDimensionListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SegmentationDimensionListener segmentationDimensionListener)
  {
    GUIManagedObjectListener superListener = null;
    if (segmentationDimensionListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { segmentationDimensionListener.segmentationDimensionActivated((SegmentationDimension) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { segmentationDimensionListener.segmentationDimensionDeactivated(guiManagedObjectID); }
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
    result.put("icon", guiManagedObject.getJSONRepresentation().get("icon"));
    result.put("name", guiManagedObject.getJSONRepresentation().get("name"));
    result.put("status",guiManagedObject.getJSONRepresentation().get("status"));
    result.put("targetingType", guiManagedObject.getJSONRepresentation().get("targetingType"));
    result.put("noOfSegments",guiManagedObject.getJSONRepresentation().get("numberOfSegments"));
    result.put("description",guiManagedObject.getJSONRepresentation().get("description"));
    return result;
  }

  /*****************************************
  *
  *  getSegmentationDimensions
  *
  *****************************************/

  public String generateSegmentationDimensionID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSegmentationDimension(String segmentationDimensionID) { return getStoredGUIManagedObject(segmentationDimensionID); }
  public Collection<GUIManagedObject> getStoredSegmentationDimensions() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSegmentationDimension(GUIManagedObject segmentationDimensionUnchecked, Date date) { return isActiveGUIManagedObject(segmentationDimensionUnchecked, date); }
  public SegmentationDimension getActiveSegmentationDimension(String segmentationDimensionID, Date date) { return (SegmentationDimension) getActiveGUIManagedObject(segmentationDimensionID, date); }
  public Collection<SegmentationDimension> getActiveSegmentationDimensions(Date date) { return (Collection<SegmentationDimension>) getActiveGUIManagedObjects(date); }

  //
  //  getSegment
  //

  public Segment getSegment(String segmentID) { synchronized (this) { return segmentsByID.get(segmentID); } }

  /*****************************************
  *
  *  putSegmentationDimension
  *
  *****************************************/

  public void putSegmentationDimension(SegmentationDimension segmentationDimension, boolean newObject, String userID) throws GUIManagerException{
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    // validate dimension
    //
    
    segmentationDimension.validate();
    
    //
    //  put
    //

    putGUIManagedObject(segmentationDimension, now, newObject, userID);
  }

  /*****************************************
  *
  *  putIncompleteOffer
  *
  *****************************************/

  public void putIncompleteSegmentationDimension(IncompleteObject offer, boolean newObject, String userID)
  {
    putGUIManagedObject(offer, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeSegmentationDimension
  *
  *****************************************/

  public void removeSegmentationDimension(String segmentationDimensionID, String userID) { removeGUIManagedObject(segmentationDimensionID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SegmentationDimensionListener
  *
  *****************************************/

  public interface SegmentationDimensionListener
  {
    public void segmentationDimensionActivated(SegmentationDimension segmentationDimension);
    public void segmentationDimensionDeactivated(String guiManagedObjectID);
  }

}
