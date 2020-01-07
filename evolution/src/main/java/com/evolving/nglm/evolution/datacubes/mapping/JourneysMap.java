package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyNode;

public class JourneysMap extends GUIManagedObjectList<Journey>
{
  protected static final Logger log = LoggerFactory.getLogger(JourneysMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneysMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();

    // Retrieve all journeys 
    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getJourneyList", "journeys"))
      {
        try
          {
            Journey journey = new Journey(item, GUIManagedObjectType.Journey);
            guiManagedObjects.put(journey.getGUIManagedObjectID(), journey);
          } 
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some journeys: {}",e.getMessage());
          }
      }
    
    // Retrieve all campaigns 
    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getCampaignList", "campaigns"))
      {
        try
          {
            Journey journey = new Journey(item, GUIManagedObjectType.Campaign);
            guiManagedObjects.put(journey.getGUIManagedObjectID(), journey);
          } 
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some campaigns: {}",e.getMessage());
          }
      }
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getNodeDisplay(String journeyID, String nodeID, String fieldName)
  {
    JourneyNode node = null;
    Journey result = this.guiManagedObjects.get(journeyID);
    if(result != null) {
      node = result.getJourneyNodes().get(nodeID);
    }
      
    if(node != null) {
      return node.getNodeName();
    } else {
      logWarningOnlyOnce("Unable to retrieve " + fieldName + ".display for " + fieldName + ".id: " + nodeID + " for journey.id: " + journeyID);
      return nodeID; // When missing, return the nodeID by default.
    }
  }
}