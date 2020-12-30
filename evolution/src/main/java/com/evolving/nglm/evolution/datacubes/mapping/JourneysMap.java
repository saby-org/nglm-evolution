package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyNode;
import com.evolving.nglm.evolution.JourneyService;

public class JourneysMap extends GUIManagedObjectMap<Journey>
{
  protected static final Logger log = LoggerFactory.getLogger(JourneysMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private JourneyService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public JourneysMap(JourneyService service) {
    super(Journey.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredJourneys(true, tenantID); }
  
  /*****************************************
  *
  * Getters
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
      logWarningOnlyOnce("Unable to retrieve display for " + fieldName + " id: " + nodeID + " (for journeyID: " + journeyID + ").");
      return nodeID; // When missing, return default.
    }
  }
  
  public long getStartDateTime(String journeyID)
  {
    Journey journey = this.guiManagedObjects.get(journeyID);
    if(journey != null) {
      return journey.getEffectiveStartDate().getTime();
    } else {
      log.error("Could not retrieve journey start date from journeyID "+ journeyID);
      return 0;
    }
  }
}