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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(GUIManagedObject.commonSchema().version(),2));
    for (Field field : GUIManagedObject.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("targetingType", Schema.STRING_SCHEMA);
    schemaBuilder.field("defaultSegmentID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("isSimpleProfileDimension", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("subscriberGroupEpoch", SubscriberGroupEpoch.schema());
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

  private SegmentationDimensionTargetingType targetingType;
  private String defaultSegmentID;
  private boolean isSimpleProfileDimension;
  private SubscriberGroupEpoch subscriberGroupEpoch;

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
  public SegmentationDimensionTargetingType getTargetingType() { return targetingType; }
  public String getDefaultSegmentID() { return defaultSegmentID; }
  public boolean getIsSimpleProfileDimension() { return isSimpleProfileDimension; }
  public SubscriberGroupEpoch getSubscriberGroupEpoch() { return subscriberGroupEpoch; }
  
  //
  //  abstract 
  //

  public abstract List<? extends Segment> getSegments();
  protected abstract String retrieveDefaultSegmentID();
  
  //
  //  setters
  //

  public void setSubscriberGroupEpoch(SubscriberGroupEpoch subscriberGroupEpoch) { this.subscriberGroupEpoch = subscriberGroupEpoch; }
  
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
    SegmentationDimensionTargetingType targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(valueStruct.getString("targetingType"));
    String defaultSegmentID = valueStruct.getString("defaultSegmentID");
    boolean isSimpleProfileDimension = valueStruct.getBoolean("isSimpleProfileDimension");
    SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpoch.unpack(new SchemaAndValue(schema.field("subscriberGroupEpoch").schema(), valueStruct.get("subscriberGroupEpoch")));
    
    //
    //  return
    //

    this.targetingType = targetingType;
    this.defaultSegmentID = defaultSegmentID;
    this.isSimpleProfileDimension = isSimpleProfileDimension;
    this.subscriberGroupEpoch = subscriberGroupEpoch;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void packCommon(Struct struct, SegmentationDimension segmentationDimension)
  {
    GUIManagedObject.packCommon(struct, segmentationDimension);
    struct.put("targetingType", segmentationDimension.getTargetingType().getExternalRepresentation());
    struct.put("defaultSegmentID", segmentationDimension.getDefaultSegmentID());
    struct.put("isSimpleProfileDimension", segmentationDimension.getIsSimpleProfileDimension());
    struct.put("subscriberGroupEpoch", SubscriberGroupEpoch.pack(segmentationDimension.getSubscriberGroupEpoch()));
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

    this.targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true));
    this.defaultSegmentID = JSONUtilities.decodeString(jsonRoot, "defaultSegmentID", false);
    this.isSimpleProfileDimension = JSONUtilities.decodeBoolean(jsonRoot, "isSimpleProfileDimension", Boolean.FALSE);
    this.subscriberGroupEpoch = new SubscriberGroupEpoch(this.getSegmentationDimensionID());
  }

  /*****************************************
  *
  *  validation
  *
  *****************************************/
  
  public boolean validate() throws GUIManagerException
  {
    this.defaultSegmentID = retrieveDefaultSegmentID();
    return true;
  }
  
  /*****************************************
  *
  *  updateSubscriberGroupEpoch
  *
  *****************************************/

  public void updateSubscriberGroupEpoch()
  {
    this.subscriberGroupEpoch = new SubscriberGroupEpoch(this.subscriberGroupEpoch);
  }

  /*****************************************
  *
  *  getDependentOnExtendedSubscriberProfile
  *
  *****************************************/

  public boolean getDependentOnExtendedSubscriberProfile()
  {
    boolean result = false;
    for (Segment segment : getSegments())
      {
        result = result || segment.getDependentOnExtendedSubscriberProfile();
      }
    return result;
  }
}
