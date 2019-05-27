/*****************************************************************************
*
*  PropensityRule.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Date;

public class PropensityRule
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(PropensityRule.class);
  
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
    schemaBuilder.name("propensityrule");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("selectedDimensions", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("size",Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PropensityRule> serde = new ConnectSerde<PropensityRule>(schema, false, PropensityRule.class, PropensityRule::pack, PropensityRule::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PropensityRule> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<String> selectedDimensions;
  private Integer size;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public List<String> getSelectedDimensions() { return selectedDimensions; }
  public Integer getSize(){return size; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PropensityRule(List<String> selectedDimensions, Integer size)
  {
    this.selectedDimensions = selectedDimensions;
    this.size = size;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensityRule propensityRule = (PropensityRule) value;
    Struct struct = new Struct(schema);
    struct.put("selectedDimensions", propensityRule.getSelectedDimensions());
    struct.put("size", propensityRule.getSize());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensityRule unpack(SchemaAndValue schemaAndValue)
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
    List<String> selectedDimensions = (List<String>) valueStruct.get("selectedDimensions");
    Integer size = valueStruct.getInt32("size");

    //
    //  return
    //

    return new PropensityRule(selectedDimensions, size);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PropensityRule(JSONObject jsonRoot) throws JSONUtilitiesException
  {
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.selectedDimensions = decodeSelectedDimensions(JSONUtilities.decodeJSONArray(jsonRoot, "selectedDimensions", true));
    this.size = JSONUtilities.decodeInteger(jsonRoot,"size",false);
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private List<String> decodeSelectedDimensions(JSONArray jsonArray) throws JSONUtilitiesException
  {
    List<String> selectedDimensions = new ArrayList<String>() ;
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject selectedDimensionJSON = (JSONObject) jsonArray.get(i);
        selectedDimensions.add(JSONUtilities.decodeString(selectedDimensionJSON, "id", true));
      }
    return selectedDimensions;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(SegmentationDimensionService segmentationDimensionService) throws GUIManagerException
  {
    /*****************************************
    *
    *  segmentationDimensions
    *   
    *****************************************/

    for (String segmentationDimensionID : selectedDimensions)
      {
        /*****************************************
        *
        *  retrieve segmentationDimension
        *
        *****************************************/
      
        Date now = SystemTime.getCurrentTime();
        SegmentationDimension segmentationDimension = segmentationDimensionService.getActiveSegmentationDimension(segmentationDimensionID, now);
        
        /*****************************************
        *
        *  validate the segmentationDimenstion exists and is active
        *
        *****************************************/

        if (segmentationDimension == null)
          {
            log.error("propensityRule uses unknown segmentation dimension: {}", segmentationDimensionID);
            throw new GUIManagerException("unknown segmentation dimension", segmentationDimensionID);
          }
      }
  }
}
