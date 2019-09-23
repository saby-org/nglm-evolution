/*****************************************************************************
*
*  SegmentationDimensionFileImport.java
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

public class SegmentationDimensionFileImport extends SegmentationDimension
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
    schemaBuilder.name("segmentation_dimension_file_import");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(SegmentationDimension.commonSchema().version(),1));
    for (Field field : SegmentationDimension.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dimensionFileID", Schema.STRING_SCHEMA);
    schemaBuilder.field("segments", SchemaBuilder.array(SegmentFileImport.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SegmentationDimensionFileImport> serde = new ConnectSerde<SegmentationDimensionFileImport>(schema, false, SegmentationDimensionFileImport.class, SegmentationDimensionFileImport::pack, SegmentationDimensionFileImport::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SegmentationDimensionFileImport> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String dimensionFileID;
  private List<SegmentFileImport> segments;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getDimensionFileID() { return dimensionFileID; }

  //
  //  abstract
  //

  @Override public List<SegmentFileImport> getSegments() { return segments; }
  @Override public String retrieveDefaultSegmentID() { return null; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationDimensionFileImport(SchemaAndValue schemaAndValue, String dimensionFileID, List<SegmentFileImport> segments)
  {
    super(schemaAndValue);
    this.dimensionFileID = dimensionFileID;
    this.segments = segments;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentationDimensionFileImport segmentationDimension = (SegmentationDimensionFileImport) value;
    Struct struct = new Struct(schema);
    SegmentationDimension.packCommon(struct, segmentationDimension);
    struct.put("dimensionFileID", segmentationDimension.getDimensionFileID());
    struct.put("segments", packSegments(segmentationDimension.getSegments()));
    return struct;
  }

  /****************************************
  *
  *  packSegments
  *
  ****************************************/

  private static List<Object> packSegments(List<SegmentFileImport> segments)
  {
    List<Object> result = new ArrayList<Object>();
    for (SegmentFileImport segment : segments)
      {
        result.add(SegmentFileImport.pack(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentationDimensionFileImport unpack(SchemaAndValue schemaAndValue)
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
    String dimensionFileID = valueStruct.getString("dimensionFileID");
    List<SegmentFileImport> segments = unpackSegments(schema.field("segments").schema(), valueStruct.get("segments"));
    
    //
    //  return
    //

    return new SegmentationDimensionFileImport(schemaAndValue, dimensionFileID, segments);
  }
  
  /*****************************************
  *
  *  unpackSegments
  *
  *****************************************/

  private static List<SegmentFileImport> unpackSegments(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema segmentSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<SegmentFileImport> result = new ArrayList<SegmentFileImport>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterion : valueArray)
      {
        result.add(SegmentFileImport.unpack(new SchemaAndValue(segmentSchema, criterion)));
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

  public SegmentationDimensionFileImport(SegmentationDimensionService segmentationDimensionService, JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentationDimensionUnchecked) throws GUIManagerException
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

    SegmentationDimensionFileImport existingSegmentationDimension = (existingSegmentationDimensionUnchecked != null && existingSegmentationDimensionUnchecked instanceof SegmentationDimensionFileImport) ? (SegmentationDimensionFileImport) existingSegmentationDimensionUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.dimensionFileID = JSONUtilities.decodeString(jsonRoot, "dimensionFileID", true);
    this.segments = decodeSegments(segmentationDimensionService, JSONUtilities.decodeJSONArray(jsonRoot, "segments", true));
    
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

  private List<SegmentFileImport> decodeSegments(SegmentationDimensionService segmentationDimensionService, JSONArray jsonArray) throws GUIManagerException
   {
    List<SegmentFileImport> result = new ArrayList<SegmentFileImport>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject segment = (JSONObject) jsonArray.get(i);
        String segmentID = JSONUtilities.decodeString(segment, "id", false);
        if (segmentID == null)
          {
            segmentID = segmentationDimensionService.generateSegmentID();
            segment.put("id", segmentID);
          }
        result.add(new SegmentFileImport(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SegmentationDimensionFileImport existingSegmentationDimension)
  {
    if (existingSegmentationDimension != null && existingSegmentationDimension.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionID(), existingSegmentationDimension.getSegmentationDimensionID());
        epochChanged = epochChanged || ! Objects.equals(getSegmentationDimensionName(), existingSegmentationDimension.getSegmentationDimensionName());
        epochChanged = epochChanged || ! Objects.equals(getTargetingType(), existingSegmentationDimension.getTargetingType());
        epochChanged = epochChanged || ! Objects.equals(dimensionFileID, existingSegmentationDimension.getDimensionFileID());
        epochChanged = epochChanged || ! Objects.equals(segments, existingSegmentationDimension.getSegments());
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
  
  @Override public boolean validate() throws GUIManagerException
  {
    //
    //  TODO : check mandatory fields (if any ...)
    //  throw new GUIManagerException("missing required calling channel properties", callingChannel.getGUIManagedObjectID())

    return true;
  }
}
