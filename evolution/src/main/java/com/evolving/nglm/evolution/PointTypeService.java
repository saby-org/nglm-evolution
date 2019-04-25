/****************************************************************************
*
*  PointTypeService.java
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

public class PointTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PointTypeService.class);

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

  public PointTypeService(String bootstrapServers, String groupID, String pointTypeTopic, boolean masterService, PointTypeListener pointTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "pointTypeService", groupID, pointTypeTopic, masterService, getSuperListener(pointTypeListener), "putPointType", "removePointType", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public PointTypeService(String bootstrapServers, String groupID, String pointTypeTopic, boolean masterService, PointTypeListener pointTypeListener)
  {
    this(bootstrapServers, groupID, pointTypeTopic, masterService, pointTypeListener, true);
  }

  //
  //  constructor
  //

  public PointTypeService(String bootstrapServers, String groupID, String pointTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, pointTypeTopic, masterService, (PointTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PointTypeListener pointTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (pointTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { pointTypeListener.pointTypeActivated((PointType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { pointTypeListener.pointTypeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getPointTypes
  *
  *****************************************/

  public String generatePointTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPointType(String pointTypeID) { return getStoredGUIManagedObject(pointTypeID); }
  public Collection<GUIManagedObject> getStoredPointTypes() { return getStoredGUIManagedObjects(); }
  public boolean isActivePointType(GUIManagedObject pointTypeUnchecked, Date date) { return isActiveGUIManagedObject(pointTypeUnchecked, date); }
  public PointType getActivePointType(String pointTypeID, Date date) { return (PointType) getActiveGUIManagedObject(pointTypeID, date); }
  public Collection<PointType> getActivePointTypes(Date date) { return (Collection<PointType>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putPointType
  *
  *****************************************/

  public void putPointType(PointType pointType, boolean newObject, String userID) throws GUIManagerException{
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(pointType, now, newObject, userID);
  }

  /*****************************************
  *
  *  putIncompleteOffer
  *
  *****************************************/

  public void putIncompletePointType(IncompleteObject offer, boolean newObject, String userID)
  {
    putGUIManagedObject(offer, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removePointType
  *
  *****************************************/

  public void removePointType(String pointTypeID, String userID) { removeGUIManagedObject(pointTypeID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface PointTypeListener
  *
  *****************************************/

  public interface PointTypeListener
  {
    public void pointTypeActivated(PointType pointType);
    public void pointTypeDeactivated(String guiManagedObjectID);
  }

}
