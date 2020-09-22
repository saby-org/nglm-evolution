/*****************************************************************************
*
*  DNBOMatrix.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "dnboMatrix", serviceClass = DNBOMatrixService.class, dependencies = {  "segmentationdimension"})
public class DNBOMatrix extends GUIManagedObject
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
    schemaBuilder.name("dnbo_matrix");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("scoringTypeId", Schema.STRING_SCHEMA);
    schemaBuilder.field("dimensionId", Schema.STRING_SCHEMA);
    schemaBuilder.field("variableId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("ranges", SchemaBuilder.array(DNBOMatrixRange.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DNBOMatrix> serde = new ConnectSerde<DNBOMatrix>(schema, false, DNBOMatrix.class, DNBOMatrix::pack, DNBOMatrix::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOMatrix> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String scoringTypeId;
  private String dimensionId;
  private String variableId;
  private List<DNBOMatrixRange> ranges;
  

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getScoringTypeId() {return scoringTypeId;}
  public String getDimensionId() {return dimensionId;}
  public String getVariableId() {return variableId;}
  public List<DNBOMatrixRange> getRanges() { return ranges; }
  

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrix(SchemaAndValue schemaAndValue, String scoringTypeId, String dimensionId, String variableId, List<DNBOMatrixRange> ranges)
  {
    super(schemaAndValue);
    this.scoringTypeId = scoringTypeId;
    this.dimensionId = dimensionId;
    this.variableId = variableId;
    this.ranges = ranges;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DNBOMatrix dnboMatrix = (DNBOMatrix) value;
    Struct struct = new Struct(schema);
    packCommon(struct, dnboMatrix);
    struct.put("scoringTypeId", dnboMatrix.getScoringTypeId());
    struct.put("dimensionId", dnboMatrix.getDimensionId());
    struct.put("variableId", dnboMatrix.getVariableId());
    struct.put("ranges", packRanges(dnboMatrix.getRanges()));
    return struct;
  }

  /****************************************
  *
  *  packRanges
  *
  ****************************************/

  private static List<Object> packRanges(List<DNBOMatrixRange> dnboMatrixRanges)
  {
    List<Object> result = new ArrayList<Object>();
    for (DNBOMatrixRange dnboMatrixRange : dnboMatrixRanges)
      {
        result.add(DNBOMatrixRange.pack(dnboMatrixRange));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DNBOMatrix unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String scoringTypeId = (String) valueStruct.get("scoringTypeId");
    String dimensionId = (String) valueStruct.get("dimensionId");
    String variableId = (String) valueStruct.get("variableId");

    List<DNBOMatrixRange> ranges = unpackRanges(schema.field("ranges").schema(), valueStruct.get("ranges"));

    //
    //  return
    //

    return new DNBOMatrix(schemaAndValue, scoringTypeId, dimensionId, variableId, ranges);
  }

  /*****************************************
  *
  *  unpackRanges
  *
  *****************************************/

  private static List<DNBOMatrixRange> unpackRanges(Schema schema, Object value)
  {
    //
    //  get schema for DNBOMatrixRange
    //

    Schema dnboMatrixRangeSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<DNBOMatrixRange> result = new ArrayList<DNBOMatrixRange>();
    List<Object> valueArray = (List<Object>) value;
    for (Object dnboMatrixRange : valueArray)
      {
        result.add(DNBOMatrixRange.unpack(new SchemaAndValue(dnboMatrixRangeSchema, dnboMatrixRange)));
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

  public DNBOMatrix(JSONObject jsonRoot, long epoch, GUIManagedObject existingDNBOMatrixUnchecked) throws GUIManagerException
  {
    
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingDNBOMatrixUnchecked != null) ? existingDNBOMatrixUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingDNBOMatrix
    *
    *****************************************/

    DNBOMatrix existingDNBOMatrix = (existingDNBOMatrixUnchecked != null && existingDNBOMatrixUnchecked instanceof DNBOMatrix) ? (DNBOMatrix) existingDNBOMatrixUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.scoringTypeId = JSONUtilities.decodeString(jsonRoot, "scoringTypeId", true);
    this.dimensionId = JSONUtilities.decodeString(jsonRoot, "dimensionId", true);
    this.variableId = JSONUtilities.decodeString(jsonRoot, "variableId", false);
    this.ranges = decodeRanges(JSONUtilities.decodeJSONArray(jsonRoot, "ranges", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    //
    //  validate ranges
    //

    //  TBD
    
   /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingDNBOMatrix))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeRanges
  *
  *****************************************/

  private List<DNBOMatrixRange> decodeRanges(JSONArray jsonArray) throws GUIManagerException
  {
    List<DNBOMatrixRange> result = new ArrayList<DNBOMatrixRange>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new DNBOMatrixRange((JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DNBOMatrix)
      {
        DNBOMatrix dnboMatrix = (DNBOMatrix) obj;
        result = true;
        result = result && Objects.equals(scoringTypeId, dnboMatrix.getScoringTypeId());
        result = result && Objects.equals(dimensionId, dnboMatrix.getDimensionId());
        result = result && Objects.equals(variableId, dnboMatrix.getVariableId());
        result = result && Objects.equals(ranges, dnboMatrix.getRanges());
      }
    return result;
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(DNBOMatrix existingDNBOMatrix)
  {
    if (existingDNBOMatrix != null && existingDNBOMatrix.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getScoringTypeId(), existingDNBOMatrix.getScoringTypeId());
        epochChanged = epochChanged || ! Objects.equals(getDimensionId(), existingDNBOMatrix.getDimensionId());
        epochChanged = epochChanged || ! Objects.equals(getVariableId(), existingDNBOMatrix.getVariableId());
        epochChanged = epochChanged || ! Objects.equals(getRanges(), existingDNBOMatrix.getRanges());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(ScoringStrategyService scoringStrategyService, Date date) throws GUIManagerException
  {
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> segmentationDimensionIDs = new ArrayList<>();
    segmentationDimensionIDs.add(getDimensionId());
   
    result.put("segmentationdimension", segmentationDimensionIDs);
   
    return result;
  }
  
  
}
