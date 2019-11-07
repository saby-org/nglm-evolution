/*****************************************************************************
*
*  VoucherService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;

public class VoucherService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(VoucherService.class);

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
  
  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, boolean masterService, VoucherListener voucherListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "voucherService", groupID, voucherTopic, masterService, getSuperListener(voucherListener), "putVoucher", "removeVoucher", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, boolean masterService, VoucherListener voucherListener)
  {
    this(bootstrapServers, groupID, voucherTopic, masterService, voucherListener, true);
  }

  //
  //  constructor
  //

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, voucherTopic, masterService, (VoucherListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(VoucherListener voucherListener)
  {
    GUIManagedObjectListener superListener = null;
    if (voucherListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { voucherListener.voucherActivated((Voucher) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { voucherListener.voucherDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getVouchers
  *
  *****************************************/

  public String generateVoucherID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredVoucher(String voucherID) { return getStoredGUIManagedObject(voucherID); }
  public GUIManagedObject getStoredVoucher(String voucherID, boolean includeArchived) { return getStoredGUIManagedObject(voucherID, includeArchived); }
  public Collection<GUIManagedObject> getStoredVouchers() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredVouchers(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveVoucher(GUIManagedObject voucherUnchecked, Date date) { return isActiveGUIManagedObject(voucherUnchecked, date); }
  public Voucher getActiveVoucher(String voucherID, Date date) { return (Voucher) getActiveGUIManagedObject(voucherID, date); }
  public Collection<Voucher> getActiveVouchers(Date date) { return (Collection<Voucher>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putVoucher
  *
  *****************************************/

  public void putVoucher(GUIManagedObject voucher, boolean newObject, String userID) { putGUIManagedObject(voucher, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeVoucher
  *
  *****************************************/

  public void removeVoucher(String voucherID, String userID) { removeGUIManagedObject(voucherID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

 @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
 {
   JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
   result.put("voucherTypeId", guiManagedObject.getJSONRepresentation().get("voucherTypeId"));
   result.put("supplierID", guiManagedObject.getJSONRepresentation().get("supplierID"));
   result.put("imageURL", guiManagedObject.getJSONRepresentation().get("imageURL"));
   return result;
 }

  /*****************************************
  *
  *  interface VoucherListener
  *
  *****************************************/

  public interface VoucherListener
  {
    public void voucherActivated(Voucher voucher);
    public void voucherDeactivated(String guiManagedObjectID);
  }

}
