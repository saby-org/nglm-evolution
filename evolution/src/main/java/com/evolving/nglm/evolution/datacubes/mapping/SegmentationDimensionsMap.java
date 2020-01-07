package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyNode;
import com.evolving.nglm.evolution.Segment;
import com.evolving.nglm.evolution.SegmentationDimension;
import com.evolving.nglm.evolution.SegmentationDimensionEligibility;
import com.evolving.nglm.evolution.SegmentationDimensionFileImport;
import com.evolving.nglm.evolution.SegmentationDimensionRanges;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;

public class SegmentationDimensionsMap extends GUIManagedObjectList<SegmentationDimension>
{
  protected static final Logger log = LoggerFactory.getLogger(SegmentationDimensionsMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SegmentationDimensionsMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();

    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getSegmentationDimensionList", "segmentationDimensions"))
      {
        SegmentationDimension segmentationDimension = null;
        try
          {
            switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(item, "targetingType", true)))
              {
                case ELIGIBILITY:
                  segmentationDimension = new SegmentationDimensionEligibility(item);
                  break;
    
                case RANGES:
                  segmentationDimension = new SegmentationDimensionRanges(item);
                  break;
    
                case FILE:
                  segmentationDimension = new SegmentationDimensionFileImport(item);
                  break;
    
                case Unknown:
                  log.warn("Unsupported dimension type {}", JSONUtilities.decodeString(item, "targetingType", false));
              }
            
            if(segmentationDimension != null) {
              this.guiManagedObjects.put(segmentationDimension.getGUIManagedObjectID(), segmentationDimension);
            }
          }
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some segmentationDimensions: {}", e.getMessage());
          }
      }
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/ 
  
  public String getSegmentDisplay(String dimensionID, String segmentID, String fieldName)
  {
    String segmentDisplay = null;
    SegmentationDimension dimension = this.guiManagedObjects.get(dimensionID);
    if(dimension != null)
      {
        for(Segment segment : dimension.getSegments()) 
          {
            if(segment.getID().equals(segmentID))
              {
                segmentDisplay = segment.getName();
                break;
              }
          }
      }
    
    if(segmentDisplay != null) {
      return segmentDisplay;
    } else {
      logWarningOnlyOnce("Unable to retrieve " + fieldName + ".display for " + fieldName + ".id: " + segmentID + " for segmentationDimension.id: " + dimensionID);
      return segmentID; // When missing, return the segmentID by default.
    }
  }
}