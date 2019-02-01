/*****************************************************************************
*
*  SegmentationDimension.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.segmentation;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public abstract class SegmentationDimension extends GUIManagedObject
{
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum SegmentationDimensionTargetingType
  {
    ELIGIBILITY,
    FILE_IMPORT,
    RANGES;
  }

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("segmentation_dimension");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("description", Schema.STRING_SCHEMA);
    schemaBuilder.field("display", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetingType", Schema.STRING_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema getCommonSchema() { return commonSchema; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String description;
  private String display;
  private SegmentationDimensionTargetingType targetingType;
  

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getSegmentationDimensionID() { return getGUIManagedObjectID(); }
  public String getSegmentationDimensionName() { return getGUIManagedObjectName(); }
  public String getDescription() { return description; }
  public String getDisplay() { return display; }
  public SegmentationDimensionTargetingType getTargetingType() { return targetingType; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationDimension(SchemaAndValue schemaAndValue, String description, String display, SegmentationDimensionTargetingType targetingType)
  {
    super(schemaAndValue);
    this.description = description;
    this.display = display;
    this.targetingType = targetingType;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void getPackCommon(Struct struct, SegmentationDimension segmentationDimension)
  {
    packCommon(struct, segmentationDimension);
    struct.put("description", segmentationDimension.getDescription());
    struct.put("display", segmentationDimension.getDisplay());
    struct.put("targetingType", segmentationDimension.getTargetingType().toString());
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SegmentationDimension(JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentationDimensionUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSegmentationDimensionUnchecked != null) ? existingSegmentationDimensionUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.description = JSONUtilities.decodeString(jsonRoot, "description", true);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", true);
    this.targetingType = SegmentationDimensionTargetingType.valueOf(JSONUtilities.decodeString(jsonRoot, "targetingType", true));
  }

}
