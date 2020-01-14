package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Offer;

public class OffersMap extends GUIManagedObjectList<Offer>
{
  protected static final Logger log = LoggerFactory.getLogger(OffersMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public OffersMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();

    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getOfferList", "offers"))
      {
        try
          {
            Offer offer = new Offer(item);
            guiManagedObjects.put(offer.getGUIManagedObjectID(), offer);
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
}