package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferObjective;
import com.evolving.nglm.evolution.OfferObjectiveInstance;
import com.evolving.nglm.evolution.OfferObjectiveService;

public class OfferObjectivesMap extends GUIManagedObjectMap<OfferObjective>
{
  protected static final Logger log = LoggerFactory.getLogger(OfferObjectivesMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private OfferObjectiveService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public OfferObjectivesMap(OfferObjectiveService service) {
    super(OfferObjective.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  @Override protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredOfferObjectives(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public Set<String> getOfferObjectiveDisplaySet(Set<String> offerObjectivesID, String fieldName)
  {
    Set<String> result = new HashSet<String>();
    for(String id: offerObjectivesID) {
      result.add(this.getDisplay(id, fieldName));
    }
    return result;
  }
}