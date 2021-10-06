package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge;
import com.evolving.nglm.evolution.LoyaltyProgramPoints;
import com.evolving.nglm.evolution.LoyaltyProgramService;

public class LoyaltyProgramChallengesMap extends GUIManagedObjectMap<LoyaltyProgramChallenge>
{
  protected static final Logger log = LoggerFactory.getLogger(LoyaltyProgramChallengesMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private LoyaltyProgramService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public LoyaltyProgramChallengesMap(LoyaltyProgramService service) {
    super(LoyaltyProgramChallenge.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  @Override protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredLoyaltyPrograms(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/  
}