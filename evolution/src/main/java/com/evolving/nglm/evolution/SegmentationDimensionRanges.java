/*****************************************************************************
*
*  SegmentationDimensionEligibility.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
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

public class SegmentationDimensionRanges extends SegmentationDimension
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
    schemaBuilder.name("segmentation_dimension_ranges");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(SegmentationDimension.commonSchema().version(),1));
    for (Field field : SegmentationDimension.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("baseSplit", SchemaBuilder.array(BaseSplit.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SegmentationDimensionRanges> serde = new ConnectSerde<SegmentationDimensionRanges>(schema, false, SegmentationDimensionRanges.class, SegmentationDimensionRanges::pack, SegmentationDimensionRanges::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SegmentationDimensionRanges> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<BaseSplit> baseSplit;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public List<BaseSplit> getBaseSplit() { return baseSplit; }

  /*****************************************
  *
  *  getSegments
  *
  *****************************************/
  
  @Override public List<SegmentRanges> getSegments()
  {
    List<SegmentRanges> result = new ArrayList<SegmentRanges>();
    for (BaseSplit currentBaseSplit : baseSplit)
      {
        result.addAll(currentBaseSplit.getSegments());
      }
    return result;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationDimensionRanges(SchemaAndValue schemaAndValue, List<BaseSplit> baseSplit)
  {
    super(schemaAndValue);
    this.baseSplit = baseSplit;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentationDimensionRanges segmentationDimension = (SegmentationDimensionRanges) value;
    Struct struct = new Struct(schema);
    SegmentationDimension.packCommon(struct, segmentationDimension);
    struct.put("baseSplit", packBaseSplit(segmentationDimension.getBaseSplit()));
    return struct;
  }

  /****************************************
  *
  *  packBaseSplit
  *
  ****************************************/

  private static List<Object> packBaseSplit(List<BaseSplit> baseSplit)
  {
    List<Object> result = new ArrayList<Object>();
    for (BaseSplit split : baseSplit)
      {
        result.add(BaseSplit.pack(split));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentationDimensionRanges unpack(SchemaAndValue schemaAndValue)
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
    List<BaseSplit> baseSplit = unpackBaseSplit(schema.field("baseSplit").schema(), valueStruct.get("baseSplit"));
    
    //
    //  return
    //

    return new SegmentationDimensionRanges(schemaAndValue, baseSplit);
  }
  
  /*****************************************
  *
  *  unpackBaseSplit
  *
  *****************************************/

  private static List<BaseSplit> unpackBaseSplit(Schema schema, Object value)
  {
    //
    //  get schema for BaseSplit
    //

    Schema segmentSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<BaseSplit> result = new ArrayList<BaseSplit>();
    List<Object> valueArray = (List<Object>) value;
    for (Object split : valueArray)
      {
        result.add(BaseSplit.unpack(new SchemaAndValue(segmentSchema, split)));
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

  public SegmentationDimensionRanges(SegmentationDimensionService segmentationDimensionService, JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentationDimensionUnchecked) throws GUIManagerException
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

    SegmentationDimensionRanges existingSegmentationDimension = (existingSegmentationDimensionUnchecked != null && existingSegmentationDimensionUnchecked instanceof SegmentationDimensionRanges) ? (SegmentationDimensionRanges) existingSegmentationDimensionUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.baseSplit = decodeBaseSplit(segmentationDimensionService, JSONUtilities.decodeJSONArray(jsonRoot, "baseSplit", true));
    
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
  *  decodeBaseSplit
  *
  *****************************************/

  private List<BaseSplit> decodeBaseSplit(SegmentationDimensionService segmentationDimensionService, JSONArray jsonArray) throws GUIManagerException
   {
    List<BaseSplit> result = new ArrayList<BaseSplit>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new BaseSplit(segmentationDimensionService, (JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SegmentationDimensionRanges existingSegmentationDimension)
  {
    if (existingSegmentationDimension != null && existingSegmentationDimension.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionID(), existingSegmentationDimension.getSegmentationDimensionID());
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionName(), existingSegmentationDimension.getSegmentationDimensionName());
        epochChanged = epochChanged || ! Objects.equals(getTargetingType(), existingSegmentationDimension.getTargetingType());
        epochChanged = epochChanged || ! Objects.equals(baseSplit, existingSegmentationDimension.getBaseSplit());
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
    //TODO : need to check that rangeMin and rangeMax are valid integers ???
    //throw new GUIManagerException("missing required calling channel properties", callingChannel.getGUIManagedObjectID())
    return super.validate();
  }

  /*****************************************
  *
  *  retrieveDefaultSegmentID
  *
  *****************************************/
  
  @Override public String retrieveDefaultSegmentID()
  {
    String defaultSegmentID = null;
    for(BaseSplit split : baseSplit)
      {
        if ((split.getProfileCriteria() == null || split.getProfileCriteria().isEmpty()) && (split.getVariableName() == null || split.getVariableName().isEmpty()) && split.getSegments().size() == 1)
          {
            SegmentRanges segment = split.getSegments().get(0);
            defaultSegmentID = segment.getID();
            break;
          }
      }   
    return defaultSegmentID;
  }
}
