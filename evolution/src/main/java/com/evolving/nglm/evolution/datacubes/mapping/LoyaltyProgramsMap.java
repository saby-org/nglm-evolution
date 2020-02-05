package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgramPoints;
import com.evolving.nglm.evolution.LoyaltyProgramService;

public class LoyaltyProgramsMap extends GUIManagedObjectMap<LoyaltyProgramPoints>
{
  protected static final Logger log = LoggerFactory.getLogger(LoyaltyProgramsMap.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private LoyaltyProgramService service;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public LoyaltyProgramsMap(LoyaltyProgramService service) {
    super(LoyaltyProgramPoints.class);
    this.service = service;
  }
  
  /*****************************************
  *
  *  getCollection
  *
  *****************************************/
  
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection() { return this.service.getStoredLoyaltyPrograms(true); }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/  
  
  public String getRewardPointsID(String id, String fieldName)
  {
    LoyaltyProgramPoints result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getRewardPointsID();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve "+fieldName+".rewardsID for "+fieldName+".id: " + id);
        return null; // When missing, return null
      }
  }
}