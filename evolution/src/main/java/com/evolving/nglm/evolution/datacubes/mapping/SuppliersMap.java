package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.PaymentMean;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.Supplier;
import com.evolving.nglm.evolution.SupplierService;

public class SuppliersMap extends GUIManagedObjectMap<Supplier>
{
  protected static final Logger log = LoggerFactory.getLogger(SuppliersMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SupplierService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public SuppliersMap(SupplierService service) {
    super(Supplier.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredSuppliers(true, tenantID); }
  
 
  
}