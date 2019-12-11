package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class DeliverableDisplayMapping extends DisplayMapping<String>
{
  public static final String ESIndex = "mapping_deliverables";
  
  public DeliverableDisplayMapping() 
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
  
  public String getDisplay(String id)
  {
    String result = this.mapping.get(id);
    if(result != null)
      {
        return result;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve deliverable.display for deliverable.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
