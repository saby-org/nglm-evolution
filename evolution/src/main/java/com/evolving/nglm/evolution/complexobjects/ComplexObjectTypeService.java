/****************************************************************************
*
*  ComplexObjectTypeService.java
*
****************************************************************************/

package com.evolving.nglm.evolution.complexobjects;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService;

public class ComplexObjectTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ComplexObjectTypeService.class);

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

  public ComplexObjectTypeService(String bootstrapServers, String groupID, String complexObjectTypeTopic, boolean masterService, ComplexObjectTypeListener complexObjectTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "complexObjectTypeService", groupID, complexObjectTypeTopic, masterService, getSuperListener(complexObjectTypeListener), "putComplexObjectType", "removeComplexObjectType", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public ComplexObjectTypeService(String bootstrapServers, String groupID, String complexObjectTypeTopic, boolean masterService, ComplexObjectTypeListener complexObjectTypeListener)
  {
    this(bootstrapServers, groupID, complexObjectTypeTopic, masterService, complexObjectTypeListener, true);
  }

  //
  //  constructor
  //

  public ComplexObjectTypeService(String bootstrapServers, String groupID, String complexObjectTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, complexObjectTypeTopic, masterService, (ComplexObjectTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ComplexObjectTypeListener complexObjectTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (complexObjectTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { complexObjectTypeListener.complexObjectTypeActivated((ComplexObjectType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { complexObjectTypeListener.complexObjectTypeDeactivated(guiManagedObjectID); }
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
  *  getComplexObjectTypes
  *
  *****************************************/

  public String generateComplexObjectTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredComplexObjectType(String complexObjectTypeID) { return getStoredGUIManagedObject(complexObjectTypeID); }
  public GUIManagedObject getStoredComplexObjectType(String complexObjectTypeID, boolean includeArchived) { return getStoredGUIManagedObject(complexObjectTypeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredComplexObjectTypes() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredComplexObjectTypes(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveComplexObjectType(GUIManagedObject complexObjectTypeUnchecked, Date date) { return isActiveGUIManagedObject(complexObjectTypeUnchecked, date); }
  public ComplexObjectType getActiveComplexObjectType(String complexObjectTypeID, Date date) { return (ComplexObjectType) getActiveGUIManagedObject(complexObjectTypeID, date); }
  public Collection<ComplexObjectType> getActiveComplexObjectTypes(Date date) { return (Collection<ComplexObjectType>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putComplexObjectType
  *
  *****************************************/

  public void putComplexObjectType(ComplexObjectType complexObjectType, boolean newObject, String userID) throws GUIManagerException{
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();
    
    //
    // Make checks: not more than 5 complex fields in total and not more than 100 availableNames 
    //
    
    Collection<ComplexObjectType> complexObjectTypes = getActiveComplexObjectTypes(now);
    
    
    boolean alreadyContainsThis = false;
    if(complexObjectTypes != null)
      {
        for(ComplexObjectType current : complexObjectTypes)
          {
            if(current.getComplexObjectTypeID().equals(complexObjectType.getComplexObjectTypeID())) { alreadyContainsThis = true; break; }
          }
      }
    
    if(!alreadyContainsThis && complexObjectTypes != null && complexObjectTypes.size() >= 5)
      {
        throw new GUIManagerException("putComplexObjectType Can't put " + complexObjectType + " because only a total of 5 complex field is allowed", "");
      }
    
    if(complexObjectType.getAvailableElements() != null && complexObjectType.getAvailableElements().size() > 100)
      {
        throw new GUIManagerException("putComplexObjectType Can't put " + complexObjectType + " because only a total of 100 elements is allowed, here there are " + complexObjectType.getAvailableElements().size(), ""); 
      }    

    //
    //  put
    //

    putGUIManagedObject(complexObjectType, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putIncompleteOffer
  *
  *****************************************/

  public void putIncompleteComplexObjectType(IncompleteObject offer, boolean newObject, String userID)
  {
    putGUIManagedObject(offer, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeComplexObjectType
  *
  *****************************************/

  public void removeComplexObjectType(String complexObjectTypeID, String userID) { 
    removeGUIManagedObject(complexObjectTypeID, SystemTime.getCurrentTime(), userID);
  }

  /*****************************************
  *
  *  interface ComplexObjectTypeListener
  *
  *****************************************/

  public interface ComplexObjectTypeListener
  {
    public void complexObjectTypeActivated(ComplexObjectType complexObjectType);
    public void complexObjectTypeDeactivated(String guiManagedObjectID);
  }

}
