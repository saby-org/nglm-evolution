package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class DeliverablesMap extends ESObjectList<String>
{
  public static final String ESIndex = "mapping_deliverables";
  
  public DeliverablesMap() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("deliverableID"), (String) row.get("deliverableName"));
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/
  
  public String getDisplay(String id, String fieldName)
  {
    String result = this.mapping.get(id);
    if(result != null)
      {
        return result;
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve "+fieldName+".display for "+fieldName+".id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
