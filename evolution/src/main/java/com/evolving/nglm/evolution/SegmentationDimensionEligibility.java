/*****************************************************************************
*
*  SegmentationDimensionEligibility.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SegmentationDimensionEligibility extends SegmentationDimension
{
  
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("segmentation_dimension_eligibility");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(SegmentationDimension.commonSchema().version(),1));
    for (Field field : SegmentationDimension.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("segments", SchemaBuilder.array(SegmentEligibility.schema()).schema());
    schemaBuilder.field("usingContactPolicy", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SegmentationDimensionEligibility> serde = new ConnectSerde<SegmentationDimensionEligibility>(schema, false, SegmentationDimensionEligibility.class, SegmentationDimensionEligibility::pack, SegmentationDimensionEligibility::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SegmentationDimensionEligibility> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<SegmentEligibility> segments;
  private boolean usingContactPolicy;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  @Override public List<SegmentEligibility> getSegments() { return segments; }
  public boolean isUsingContactPolicy() { return usingContactPolicy; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationDimensionEligibility(SchemaAndValue schemaAndValue, List<SegmentEligibility> segments, boolean usingContactPolicy)
  {
    super(schemaAndValue);
    this.segments = segments;
    this.usingContactPolicy = usingContactPolicy;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentationDimensionEligibility segmentationDimension = (SegmentationDimensionEligibility) value;
    Struct struct = new Struct(schema);
    SegmentationDimension.packCommon(struct, segmentationDimension);
    struct.put("segments", packSegments(segmentationDimension.getSegments()));
    struct.put("usingContactPolicy", segmentationDimension.isUsingContactPolicy());
    return struct;
  }

  /****************************************
  *
  *  packSegments
  *
  ****************************************/

  private static List<Object> packSegments(List<SegmentEligibility> segments)
  {
    List<Object> result = new ArrayList<Object>();
    for (SegmentEligibility segment : segments)
      {
        result.add(SegmentEligibility.pack(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentationDimensionEligibility unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    List<SegmentEligibility> segments = unpackSegments(schema.field("segments").schema(), valueStruct.get("segments"));
    boolean usingContactPolicy = valueStruct.getBoolean("usingContactPolicy");
    //
    //  return
    //

    return new SegmentationDimensionEligibility(schemaAndValue, segments, usingContactPolicy);
  }
  
  /*****************************************
  *
  *  unpackSegments
  *
  *****************************************/

  private static List<SegmentEligibility> unpackSegments(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema segmentSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<SegmentEligibility> result = new ArrayList<SegmentEligibility>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterion : valueArray)
      {
        result.add(SegmentEligibility.unpack(new SchemaAndValue(segmentSchema, criterion)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SegmentationDimensionEligibility(SegmentationDimensionService segmentationDimensionService, JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentationDimensionUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, epoch, existingSegmentationDimensionUnchecked);

    /*****************************************
    *
    *  existingSegmentationDimension
    *
    *****************************************/

    SegmentationDimensionEligibility existingSegmentationDimension = (existingSegmentationDimensionUnchecked != null && existingSegmentationDimensionUnchecked instanceof SegmentationDimensionEligibility) ? (SegmentationDimensionEligibility) existingSegmentationDimensionUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.segments = decodeSegments(segmentationDimensionService, JSONUtilities.decodeJSONArray(jsonRoot, "segments", true));
    this.usingContactPolicy = JSONUtilities.decodeBoolean(jsonRoot, "usingContactPolicy", Boolean.FALSE);
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSegmentationDimension))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeSegments
  *
  *****************************************/

  private List<SegmentEligibility> decodeSegments(SegmentationDimensionService segmentationDimensionService, JSONArray jsonArray) throws GUIManagerException
   {
    List<SegmentEligibility> result = new ArrayList<SegmentEligibility>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject segment = (JSONObject) jsonArray.get(i);
        String segmentID = JSONUtilities.decodeString(segment, "id", false);
        if (segmentID == null)
          {
            segmentID = segmentationDimensionService.generateSegmentationDimensionID();
            segment.put("id", segmentID);
          }
        result.add(new SegmentEligibility(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SegmentationDimensionEligibility existingSegmentationDimension)
  {
    if (existingSegmentationDimension != null && existingSegmentationDimension.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionID(), existingSegmentationDimension.getSegmentationDimensionID());
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionName(), existingSegmentationDimension.getSegmentationDimensionName());
        epochChanged = epochChanged || ! Objects.equals(getTargetingType(), existingSegmentationDimension.getTargetingType());
        epochChanged = epochChanged || ! Objects.equals(segments, existingSegmentationDimension.getSegments());
        epochChanged = epochChanged || ! Objects.equals(usingContactPolicy, existingSegmentationDimension.isUsingContactPolicy());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }

  /*****************************************
  *
  *  validation
  *
  *****************************************/
  
  @Override
  public boolean validate() throws GUIManagerException {
    //TODO : check mandatory fields (if any ...)
    //throw new GUIManagerException("missing required calling channel properties", callingChannel.getGUIManagedObjectID())
    return super.validate();
  }

  /*****************************************
  *
  *  hasDefaultSegment
  *
  *****************************************/
  
  @Override
  public boolean hasDefaultSegment(){
    boolean hasDefault = false;
    //iterate over all segments to check if it is a default one 
    for(SegmentEligibility segment : segments){
      if(segment.getProfileCriteria() == null || segment.getProfileCriteria().isEmpty()){
        hasDefault =true;
      }
    }
    return hasDefault;
  }
}
