package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class JourneyDisplayMapping extends DisplayMapping<String>
{
  public static final String ESIndex = "mapping_journeys";
  
  public JourneyDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("journeyID"), (String) row.get("journeyName"));
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/
  
  public String getDisplay(String id)
  {
    String result = this.mapping.get(id);
    if(result != null)
      {
        return result;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve journey.display for journey.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
