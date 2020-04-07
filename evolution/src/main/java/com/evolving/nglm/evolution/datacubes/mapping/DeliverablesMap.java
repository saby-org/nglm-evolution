package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class DeliverablesMap extends ESObjectList<String>
{
  public static final String ESIndex = "mapping_deliverables";

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DeliverablesMap() 
  {
    super(ESIndex);
  }

  /*****************************************
  *
  * ESObjectList implementation
  *
  *****************************************/
  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("deliverableID"), (String) row.get("deliverableName"));
  }

  /*****************************************
  *
  * Getters
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
        logWarningOnlyOnce("Unable to retrieve display for " + fieldName + " id: " + id);
        return id; // When missing, return default.
      }
  }
}
