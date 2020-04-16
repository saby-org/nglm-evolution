package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferService;

public class OffersMap extends GUIManagedObjectMap<Offer>
{
  protected static final Logger log = LoggerFactory.getLogger(OffersMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private OfferService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public OffersMap(OfferService service) {
    super(Offer.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection() { return this.service.getStoredOffers(true); }
}