package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgramMission;
import com.evolving.nglm.evolution.LoyaltyProgramService;

public class LoyaltyProgramMissionsMap extends GUIManagedObjectMap<LoyaltyProgramMission>
{
  protected static final Logger log = LoggerFactory.getLogger(LoyaltyProgramMissionsMap.class);

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
  public LoyaltyProgramMissionsMap(LoyaltyProgramService service) {
    super(LoyaltyProgramMission.class);
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