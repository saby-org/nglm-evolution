package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferObjectiveInstance;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMean;

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
  @Override protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredOffers(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public Set<String> getOfferObjectivesID(String id, String fieldName)
  {
    Offer offer = this.guiManagedObjects.get(id);
    if(offer != null)
      {
        Set<OfferObjectiveInstance> instances = offer.getOfferObjectives();
        Set<String> result = new HashSet<String>();
        for(OfferObjectiveInstance instance: instances) {
          result.add(instance.getOfferObjectiveID());
        }
        return result;
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve offer for " + fieldName + " id: " + id + ".");
        return Collections.EMPTY_SET; // When missing, return empty set.
      }
  }
}