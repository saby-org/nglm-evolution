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
  * Properties
  *
  *****************************************/
  private SegmentationDimensionService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public SegmentationDimensionsMap(SegmentationDimensionService service) {
    super(SegmentationDimension.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  @Override protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredSegmentationDimensions(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/ 
  public String getDimensionDisplay(String dimensionID, String fieldName)
  {
    SegmentationDimension dimension = this.guiManagedObjects.get(dimensionID);
    if(dimension != null && dimension.getGUIManagedObjectDisplay() != null) {
      return dimension.getGUIManagedObjectDisplay();
    } else {
      logWarningOnlyOnce("Unable to retrieve display for " + fieldName + " field term.");
      return dimensionID; // When missing, return default.
    }
  }
  
  public String getSegmentDisplay(String dimensionID, String segmentID, String fieldName)
  {
    String segmentDisplay = null;
    SegmentationDimension dimension = this.guiManagedObjects.get(dimensionID);
    if(dimension != null) {
      for(Segment segment : dimension.getSegments()) {
        if(segment.getID().equals(segmentID)) {
          segmentDisplay = segment.getName();
          break;
        }
      }
    }
    
    if(segmentDisplay != null) {
      return segmentDisplay;
    } else {
      logWarningOnlyOnce("Unable to retrieve display for " + fieldName + " id: " + segmentID + " for dimension id: " + dimensionID);
      return segmentID; // When missing, return default.
    }
  }
}