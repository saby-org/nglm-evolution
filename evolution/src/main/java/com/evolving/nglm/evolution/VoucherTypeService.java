/*****************************************************************************
*
*  VoucherTypeService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { voucherTypeListener.voucherTypeDeactivated(guiManagedObjectID); }
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
  public Collection<GUIManagedObject> getStoredVoucherTypes() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredVoucherTypes(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveVoucherType(GUIManagedObject voucherTypeUnchecked, Date date) { return isActiveGUIManagedObject(voucherTypeUnchecked, date); }
  public VoucherType getActiveVoucherType(String voucherTypeID, Date date) { return (VoucherType) getActiveGUIManagedObject(voucherTypeID, date); }
  public Collection<VoucherType> getActiveVoucherTypes(Date date) { return (Collection<VoucherType>) getActiveGUIManagedObjects(date); }

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

  public void removeVoucherType(String voucherTypeID, String userID) { removeGUIManagedObject(voucherTypeID, SystemTime.getCurrentTime(), userID); }

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
