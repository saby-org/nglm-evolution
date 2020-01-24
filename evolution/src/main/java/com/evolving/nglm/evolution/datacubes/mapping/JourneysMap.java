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
  *  data
  *
  *****************************************/
  
  private JourneyService service;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneysMap(JourneyService service) {
    super(Journey.class);
    this.service = service;
  }
  
  /*****************************************
  *
  *  getCollection
  *
  *****************************************/
  
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection() { return this.service.getStoredJourneys(true); }
  
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