package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SalesChannel;

public class SalesChannelsMap extends GUIManagedObjectList<SalesChannel>
{
  protected static final Logger log = LoggerFactory.getLogger(SalesChannelsMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SalesChannelsMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();

    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getSalesChannelList", "salesChannels"))
      {
        try
          {
            SalesChannel salesChannel = new SalesChannel(item);
            guiManagedObjects.put(salesChannel.getGUIManagedObjectID(), salesChannel);
          } 
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some salesChannels: {}",e.getMessage());
          }
      }
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
}