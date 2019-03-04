/*****************************************************************************
*
*  SegmentationDimension.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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

import java.util.List;

public abstract class SegmentationDimension extends GUIManagedObject
{
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum SegmentationDimensionTargetingType
  {
    ELIGIBILITY("ELIGIBILITY"),
    FILE_IMPORT("FILE_IMPORT"),
    RANGES("RANGES"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private SegmentationDimensionTargetingType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static SegmentationDimensionTargetingType fromExternalRepresentation(String externalRepresentation) { for (SegmentationDimensionTargetingType enumeratedValue : SegmentationDimensionTargetingType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(GUIManagedObject.commonSchema().version(),1));
    for (Field field : GUIManagedObject.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("description", Schema.STRING_SCHEMA);
    schemaBuilder.field("display", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetingType", Schema.STRING_SCHEMA);
    schemaBuilder.field("hasDefaultSegment", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("isSimpleProfileDimension", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String description;
  private String display;
  private SegmentationDimensionTargetingType targetingType;
  private boolean hasDefaultSegment;
  private boolean isSimpleProfileDimension;

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
  public boolean getHasDefaultSegment() { return hasDefaultSegment; }
  public boolean getIsSimpleProfileDimension() { return isSimpleProfileDimension; }

  //
  //  abstract
  //

  public abstract List<? extends Segment> getSegments();
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationDimension(SchemaAndValue schemaAndValue)
  {
    //
    //  superclass
    //
    
    super(schemaAndValue);
    
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String description = valueStruct.getString("description");
    String display = valueStruct.getString("display");
    SegmentationDimensionTargetingType targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(valueStruct.getString("targetingType"));
    boolean hasDefaultSegment = valueStruct.getBoolean("hasDefaultSegment");
    boolean isSimpleProfileDimension = valueStruct.getBoolean("isSimpleProfileDimension");

    //
    //  return
    //

    this.description = description;
    this.display = display;
    this.targetingType = targetingType;
    this.hasDefaultSegment = hasDefaultSegment;
    this.isSimpleProfileDimension = isSimpleProfileDimension;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void packCommon(Struct struct, SegmentationDimension segmentationDimension)
  {
    GUIManagedObject.packCommon(struct, segmentationDimension);
    struct.put("description", segmentationDimension.getDescription());
    struct.put("display", segmentationDimension.getDisplay());
    struct.put("targetingType", segmentationDimension.getTargetingType().getExternalRepresentation());
    struct.put("hasDefaultSegment", segmentationDimension.getHasDefaultSegment());
    struct.put("isSimpleProfileDimension", segmentationDimension.getIsSimpleProfileDimension());
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
    this.targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true));
    this.hasDefaultSegment = JSONUtilities.decodeBoolean(jsonRoot, "hasDefaultSegment", Boolean.FALSE);
    this.isSimpleProfileDimension = JSONUtilities.decodeBoolean(jsonRoot, "isSimpleProfileDimension", Boolean.FALSE);
  }

  /*****************************************
  *
  *  validation
  *
  *****************************************/
  
  public boolean validate() throws GUIManagerException {
    this.hasDefaultSegment = hasDefaultSegment();
    return true;
  }
  
  /*****************************************
  *
  *  hasDefaultSegment
  *
  *****************************************/
  
  //
  //  abstract
  //

  protected abstract boolean hasDefaultSegment();
  
}
