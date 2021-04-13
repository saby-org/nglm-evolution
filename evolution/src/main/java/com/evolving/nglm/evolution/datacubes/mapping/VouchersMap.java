package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Voucher;
import com.evolving.nglm.evolution.VoucherService;

public class VouchersMap extends GUIManagedObjectMap<Voucher>
{
  protected static final Logger log = LoggerFactory.getLogger(VouchersMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private VoucherService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public VouchersMap(VoucherService service) {
    super(Voucher.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredVouchers(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public String getSupplierID(String id, String fieldName)
  {
    Voucher result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getSupplierID();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve supplierID for " + fieldName + " (VoucherID: " + id + ").");
        return id; // When missing, return default.
      }
  }
  
}