/*****************************************************************************
*
*  Segment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.segmentation;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class BaseSplit 
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(BaseSplit.class);

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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("base_split");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("splitName", Schema.STRING_SCHEMA);
    schemaBuilder.field("variableName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("segments", SchemaBuilder.array(SegmentRanges.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String splitName;
  private String variableName;
  private List<EvaluationCriterion> profileCriteria;
  private List<SegmentRanges> segments;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private BaseSplit(String splitName, String variableName, List<EvaluationCriterion> profileCriteria, List<SegmentRanges> segments)
  {
    this.splitName = splitName;
    this.variableName = variableName;
    this.profileCriteria = profileCriteria;
    this.segments = segments;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  BaseSplit(SegmentationDimensionService segmentationDimensionService, JSONObject jsonRoot) throws GUIManagerException
  {
    this.splitName = JSONUtilities.decodeString(jsonRoot, "splitName", true);
    this.variableName = JSONUtilities.decodeString(jsonRoot, "variableName", false);
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", false));
    this.segments = decodeSegmentRanges(segmentationDimensionService, JSONUtilities.decodeJSONArray(jsonRoot, "segments", false));
  }

  /*****************************************
  *
  *  decodeProfileCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeProfileCriteria(JSONArray jsonArray) throws GUIManagerException
  {
    if(jsonArray == null){
      return null;
    }
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile)); //TODO SCH : what is "CriterionContext.Profile" for ?
      }
    return result;
  }

  /*****************************************
  *
  *  decodeSegmentRanges
  *
  *****************************************/

  private List<SegmentRanges> decodeSegmentRanges(SegmentationDimensionService segmentationDimensionService, JSONArray jsonArray) throws GUIManagerException
  {
    if(jsonArray == null){
      return null;
    }
    List<SegmentRanges> result = new ArrayList<SegmentRanges>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject segment = (JSONObject) jsonArray.get(i);
        String segmentID = JSONUtilities.decodeString(segment, "id", false);
        if (segmentID == null)
          {
            segmentID = segmentationDimensionService.generateSegmentationDimensionID();
            segment.put("id", segmentID);
          }
        result.add(new SegmentRanges(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSplitName() { return splitName; }
  public String getVariableName() { return variableName; }
  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }
  public List<SegmentRanges> getSegments() { return segments; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<BaseSplit> serde()
  {
    return new ConnectSerde<BaseSplit>(schema, false, BaseSplit.class, BaseSplit::pack, BaseSplit::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    BaseSplit segment = (BaseSplit) value;
    Struct struct = new Struct(schema);
    struct.put("splitName", segment.getSplitName());
    struct.put("variableName", segment.getVariableName());
    struct.put("profileCriteria", packProfileCriteria(segment.getProfileCriteria()));
    struct.put("segments", packSegmentRanges(segment.getSegments()));
    return struct;
  }

  /****************************************
  *
  *  packProfileCriteria
  *
  ****************************************/

  private static List<Object> packProfileCriteria(List<EvaluationCriterion> profileCriteria)
  {
    if(profileCriteria == null){
      return null;
    }
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : profileCriteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
      }
    return result;
  }

  /****************************************
  *
  *  packSegmentRanges
  *
  ****************************************/

  private static List<Object> packSegmentRanges(List<SegmentRanges> segments)
  {
    if(segments == null){
      return null;
    }
    List<Object> result = new ArrayList<Object>();
    for (SegmentRanges segment : segments)
      {
        result.add(SegmentRanges.pack(segment));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static BaseSplit unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but argument
    //

    if(value == null){
      return null;
    }
    Struct valueStruct = (Struct) value;
    String splitName = valueStruct.getString("splitName");
    String variableName = valueStruct.getString("variableName");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    List<SegmentRanges> segments = unpackSegmentRanges(schema.field("segments").schema(), valueStruct.get("segments"));

    //
    //  construct
    //

    BaseSplit result = new BaseSplit(splitName, variableName, profileCriteria, segments);

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackProfileCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackProfileCriteria(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema evaluationCriterionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    if(value == null){
      return null;
    }
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterion : valueArray)
      {
        result.add(EvaluationCriterion.unpack(new SchemaAndValue(evaluationCriterionSchema, criterion)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackSegmentRanges
  *
  *****************************************/

  private static List<SegmentRanges> unpackSegmentRanges(Schema schema, Object value)
  {
    //
    //  get schema for SegmentRanges
    //

    Schema evaluationCriterionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    if(value == null){
      return null;
    }
    List<SegmentRanges> result = new ArrayList<SegmentRanges>();
    List<Object> valueArray = (List<Object>) value;
    for (Object segment : valueArray)
      {
        result.add(SegmentRanges.unpack(new SchemaAndValue(evaluationCriterionSchema, segment)));
      }

    //
    //  return
    //

    return result;
  }

}
