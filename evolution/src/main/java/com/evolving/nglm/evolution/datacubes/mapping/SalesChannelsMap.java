package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.SalesChannel;
import com.evolving.nglm.evolution.SalesChannelService;

public class SalesChannelsMap extends GUIManagedObjectMap<SalesChannel>
{
  protected static final Logger log = LoggerFactory.getLogger(SalesChannelsMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SalesChannelService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  
  public SalesChannelsMap(SalesChannelService service) {
    super(SalesChannel.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredSalesChannels(true, tenantID); }
}