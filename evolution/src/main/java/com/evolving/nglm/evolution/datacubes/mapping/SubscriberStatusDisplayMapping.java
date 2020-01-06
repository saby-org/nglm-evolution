package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class SubscriberStatusDisplayMapping extends DisplayMapping<String>
{
  public static final String ESIndex = "mapping_evolutionsubscriberstatus";
  
  public SubscriberStatusDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("fieldID"), (String) row.get("fieldDisplay"));
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
        logWarningOnlyOnce(id, "Unable to retrieve evolutionSubscriberStatus.display for evolutionSubscriberStatus.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
