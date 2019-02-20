/*****************************************************************************
*
*  SegmentEligibility.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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

public class SegmentEligibility implements Segment
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentEligibility.class);

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
    schemaBuilder.name("segment_eligibility");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
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

  private String id;
  private String name;
  private List<EvaluationCriterion> profileCriteria;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private SegmentEligibility(String id, String name, List<EvaluationCriterion> profileCriteria)
  {
    this.id = id;
    this.name = name;
    this.profileCriteria = profileCriteria;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  SegmentEligibility(JSONObject jsonRoot) throws GUIManagerException
  {
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", false));
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
  *  accessors
  *
  *****************************************/

  public String getID() { return id; }
  public String getName() { return name; }
  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SegmentEligibility> serde()
  {
    return new ConnectSerde<SegmentEligibility>(schema, false, SegmentEligibility.class, SegmentEligibility::pack, SegmentEligibility::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentEligibility segment = (SegmentEligibility) value;
    Struct struct = new Struct(schema);
    struct.put("id", segment.getID());
    struct.put("name", segment.getName());
    struct.put("profileCriteria", packProfileCriteria(segment.getProfileCriteria()));
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

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentEligibility unpack(SchemaAndValue schemaAndValue)
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
    String id = valueStruct.getString("id");
    String name = valueStruct.getString("name");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));

    //
    //  construct
    //

    SegmentEligibility result = new SegmentEligibility(id, name, profileCriteria);

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

}
