package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;
import java.util.Set;

public class LoyaltyProgramDisplayMapping extends DisplayMapping<LoyaltyProgramInformation>
{
  public static final String ESIndex = "mapping_loyaltyprograms";
  
  public LoyaltyProgramDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("loyaltyProgramID"), new LoyaltyProgramInformation((String) row.get("loyaltyProgramName"), (String) row.get("rewardPointsID")));
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/
  
  public String getDisplay(String id)
  {
    LoyaltyProgramInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.loyaltyProgramDisplay;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve loyaltyProgram.display and loyaltyProgram.rewardPointsID for loyaltyProgram.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
  
  public Set<String> getLoyaltyPrograms()
  {
    return this.mapping.keySet();
  }
  
  public String getRewardPointsID(String id)
  {
    LoyaltyProgramInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.rewardPointsID;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve loyaltyProgram.display and loyaltyProgram.rewardsID for loyaltyProgram.id: " + id);
        return null; // When missing, return null
      }
  }
}
