package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Segment;
import com.evolving.nglm.evolution.SegmentationDimension;
import com.evolving.nglm.evolution.SegmentationDimensionService;

public class SegmentationDimensionsMap extends GUIManagedObjectMap<SegmentationDimension>
{
  protected static final Logger log = LoggerFactory.getLogger(SegmentationDimensionsMap.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private SegmentationDimensionService service;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SegmentationDimensionsMap(SegmentationDimensionService service) {
    super(SegmentationDimension.class);
    this.service = service;
  }
  
  /*****************************************
  *
  *  getCollection
  *
  *****************************************/
  
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection() { return this.service.getStoredSegmentationDimensions(true); }
  
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