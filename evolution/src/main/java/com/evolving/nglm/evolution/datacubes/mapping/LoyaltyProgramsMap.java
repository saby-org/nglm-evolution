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
  * Properties
  *
  *****************************************/
  private LoyaltyProgramService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public LoyaltyProgramsMap(LoyaltyProgramService service) {
    super(LoyaltyProgramPoints.class);
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
  public String getRewardPointsID(String id, String fieldName)
  {
    LoyaltyProgramPoints result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getRewardPointsID();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve reward name for " + fieldName + " id: " + id);
        return null; // When missing, return null
      }
  }
}