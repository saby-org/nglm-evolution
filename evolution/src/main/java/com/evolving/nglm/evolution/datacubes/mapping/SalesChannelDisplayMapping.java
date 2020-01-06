package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class SalesChannelDisplayMapping extends DisplayMapping<String>
{
  public static final String ESIndex = "mapping_saleschannels";
  
  public SalesChannelDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("salesChannelID"), (String) row.get("salesChannelName"));
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
        logWarningOnlyOnce(id, "Unable to retrieve salesChannel.display for salesChannel.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
