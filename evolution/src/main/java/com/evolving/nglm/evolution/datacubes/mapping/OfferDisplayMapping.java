package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class OfferDisplayMapping extends DisplayMapping<String>
{
  public static final String ESIndex = "mapping_offers";
  
  public OfferDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("offerID"), (String) row.get("offerName"));
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
        logWarningOnlyOnce(id, "Unable to retrieve offer.display for offer.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}
