/****************************************************************************
*
*  ComplexObjectTypeService.java
*
****************************************************************************/

package com.evolving.nglm.evolution.otp;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectType;
import com.evolving.nglm.evolution.GUIService;

public class OTPTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OTPTypeService.class);

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

  public OTPTypeService(String bootstrapServers, String groupID, String otpTypeTopic, boolean masterService, OTPTypeListener otpTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "otpTypeService", groupID, otpTypeTopic, masterService, getSuperListener(otpTypeListener), "putOTPType", "removeOTPType", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public OTPTypeService(String bootstrapServers, String groupID, String otpTypeTopic, boolean masterService, OTPTypeListener otpTypeListener)
  {
    this(bootstrapServers, groupID, otpTypeTopic, masterService, otpTypeListener, true);
  }

  //
  //  constructor
  //

  public OTPTypeService(String bootstrapServers, String groupID, String otpTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, otpTypeTopic, masterService, (OTPTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(OTPTypeListener otpTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (otpTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { otpTypeListener.otpTypeActivated((OTPType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { otpTypeListener.otpTypeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
   *
   *  getSummaryJSONRepresentation
   *
   *****************************************/

  @Override public JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    // TODO ADD more details ?
    // result.put("outfield", guiManagedObject.getJSONRepresentation().get("reprfield"));
    return result;
  }

  /*****************************************
  *
  *  getOTPTypes
  *
  *****************************************/

  public String generateOTPTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredOTPType(String otpTypeID) { return getStoredGUIManagedObject(otpTypeID); }
  public GUIManagedObject getStoredOTPType(String otpTypeID, boolean includeArchived) { return getStoredGUIManagedObject(otpTypeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredOTPTypes(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredOTPTypes(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveOTPType(GUIManagedObject otpTypeIDUnchecked, Date date) { return isActiveGUIManagedObject(otpTypeIDUnchecked, date); }
  public OTPType getActiveOTPType(String otpTypeID, Date date) { return (OTPType) getActiveGUIManagedObject(otpTypeID, date); }
  public Collection<OTPType> getActiveOTPTypes(Date date, int tenantID) { return (Collection<OTPType>) getActiveGUIManagedObjects(date, tenantID); }
  
  public OTPType getActiveOTPTypeByName(String displayName, int tenantID ) {
	// return the first active OTPType from the topic matching the (GUI display) name
		  for (OTPType candidat : getActiveOTPTypes(SystemTime.getCurrentTime(),tenantID)) {
		       if (candidat.getGUIManagedObjectDisplay().equals(displayName)) return candidat;
		  }
		  // no active element found
		  return null;
  }
  
  /*****************************************
  *
  *  putOTPType
  *
  *****************************************/

  public void putOTPType(GUIManagedObject otpType, boolean newObject, String userID, int tenantID) throws GUIManagerException{
     putGUIManagedObject(otpType, SystemTime.getCurrentTime(), newObject, userID);
  }
  
  /*****************************************
  *
  *  putIncompleteOTPType
  *
  *****************************************/

  public void putIncompleteOTPType(IncompleteObject otpType, boolean newObject, String userID)
  {
    putGUIManagedObject(otpType, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeOTPType
  *
  *****************************************/

  public void removeOTPType(String otpTypeID, String userID, int tenantID) { 
    removeGUIManagedObject(otpTypeID, SystemTime.getCurrentTime(), userID, tenantID);
  }

  /*****************************************
  *
  *  interface OTPTypeListener
  *
  *****************************************/

  public interface OTPTypeListener
  {
    public void otpTypeActivated(OTPType otpType);
    public void otpTypeDeactivated(String guiManagedObjectID);
  }

}
