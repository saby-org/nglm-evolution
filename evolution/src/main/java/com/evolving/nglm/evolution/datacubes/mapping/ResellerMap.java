package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Reseller;
import com.evolving.nglm.evolution.ResellerService;

public class ResellerMap extends GUIManagedObjectMap<Reseller>
{
  protected static final Logger log = LoggerFactory.getLogger(ResellerMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private ResellerService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  
  public ResellerMap(ResellerService service) {
    super(Reseller.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredResellers(true, tenantID); }
}