/*****************************************************************************
*
*  VoucherTypeService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;

public class VoucherTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(VoucherTypeService.class);

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
  
  public VoucherTypeService(String bootstrapServers, String groupID, String voucherTypeTopic, boolean masterService, VoucherTypeListener voucherTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "voucherTypeService", groupID, voucherTypeTopic, masterService, getSuperListener(voucherTypeListener), "putVoucherType", "removeVoucherType", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //

  public VoucherTypeService(String bootstrapServers, String groupID, String voucherTypeTopic, boolean masterService, VoucherTypeListener voucherTypeListener)
  {
    this(bootstrapServers, groupID, voucherTypeTopic, masterService, voucherTypeListener, true);
  }

  //
  //  constructor
  //

  public VoucherTypeService(String bootstrapServers, String groupID, String voucherTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, voucherTypeTopic, masterService, (VoucherTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(VoucherTypeListener voucherTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (voucherTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { voucherTypeListener.voucherTypeActivated((VoucherType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { voucherTypeListener.voucherTypeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getVoucherTypes
  *
  *****************************************/

  public String generateVoucherTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredVoucherType(String voucherTypeID) { return getStoredGUIManagedObject(voucherTypeID); }
  public GUIManagedObject getStoredVoucherType(String voucherTypeID, boolean includeArchived) { return getStoredGUIManagedObject(voucherTypeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredVoucherTypes(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredVoucherTypes(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveVoucherType(GUIManagedObject voucherTypeUnchecked, Date date) { return isActiveGUIManagedObject(voucherTypeUnchecked, date); }
  public VoucherType getActiveVoucherType(String voucherTypeID, Date date) { return (VoucherType) getActiveGUIManagedObject(voucherTypeID, date); }
  public Collection<VoucherType> getActiveVoucherTypes(Date date, int tenantID) { return (Collection<VoucherType>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putVoucherType
  *
  *****************************************/

  public void putVoucherType(GUIManagedObject voucherType, boolean newObject, String userID) { putGUIManagedObject(voucherType, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeVoucherType
  *
  *****************************************/

  public void removeVoucherType(String voucherTypeID, String userID, int tenantID) { removeGUIManagedObject(voucherTypeID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

 @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
 {
   JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
   result.put("codeType", guiManagedObject.getJSONRepresentation().get("codeType"));
   result.put("transferable", guiManagedObject.getJSONRepresentation().get("transferable"));
   return result;
 }

  /*****************************************
  *
  *  interface VoucherTypeListener
  *
  *****************************************/

  public interface VoucherTypeListener
  {
    public void voucherTypeActivated(VoucherType voucherType);
    public void voucherTypeDeactivated(String guiManagedObjectID);
  }

}
