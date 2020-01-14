package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramPoints;

public class LoyaltyProgramsMap extends GUIManagedObjectList<LoyaltyProgramPoints>
{
  protected static final Logger log = LoggerFactory.getLogger(LoyaltyProgramsMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public LoyaltyProgramsMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();

    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getLoyaltyProgramList", "loyaltyPrograms"))
      {
        try
          {
            LoyaltyProgramPoints loyaltyProgram = null;
            switch (LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(item, "loyaltyProgramType", true)))
            {
              case POINTS:
                loyaltyProgram = new LoyaltyProgramPoints(item);
                break;

              case Unknown:
                log.warn("Unsupported loyalty program type {}", JSONUtilities.decodeString(item, "loyaltyProgramType", false));
            }
            
            if(loyaltyProgram != null) {
              guiManagedObjects.put(loyaltyProgram.getGUIManagedObjectID(), loyaltyProgram);
            }
          } 
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some offers: {}",e.getMessage());
          }
      }
  }
  
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