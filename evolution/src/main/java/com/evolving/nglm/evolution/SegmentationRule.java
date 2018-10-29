/*****************************************************************************
*
*  SegmentationRule.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SegmentationRule extends GUIManagedObject
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
    schemaBuilder.name("segmentation_rule");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("segmentationRuleCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("subscriberGroupName", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SegmentationRule> serde = new ConnectSerde<SegmentationRule>(schema, false, SegmentationRule.class, SegmentationRule::pack, SegmentationRule::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SegmentationRule> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<EvaluationCriterion> segmentationRuleCriteria;
  private String subscriberGroupName;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getSegmentationRuleID() { return getGUIManagedObjectID(); }
  public List<EvaluationCriterion> getSegmentationRuleCriteria() { return segmentationRuleCriteria; }
  public String getSubscriberGroupName() { return subscriberGroupName; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentationRule(SchemaAndValue schemaAndValue, List<EvaluationCriterion> segmentationRuleCriteria, String subscriberGroupName)
  {
    super(schemaAndValue);
    this.segmentationRuleCriteria = segmentationRuleCriteria;
    this.subscriberGroupName = subscriberGroupName;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentationRule segmentationRule = (SegmentationRule) value;
    Struct struct = new Struct(schema);
    packCommon(struct, segmentationRule);
    struct.put("segmentationRuleCriteria", packSegmentationRuleCriteria(segmentationRule.getSegmentationRuleCriteria()));
    struct.put("subscriberGroupName", segmentationRule.getSubscriberGroupName());
    return struct;
  }

  /****************************************
  *
  *  packAutoTargetingCriteria
  *
  ****************************************/

  private static List<Object> packSegmentationRuleCriteria(List<EvaluationCriterion> segmentationRuleCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : segmentationRuleCriteria)
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

  public static SegmentationRule unpack(SchemaAndValue schemaAndValue)
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
    List<EvaluationCriterion> segmentationRuleCriteria = unpackSegmentationRuleCriteria(schema.field("segmentationRuleCriteria").schema(), valueStruct.get("segmentationRuleCriteria"));
    String subscriberGroupName = valueStruct.getString("subscriberGroupName");
    
    //
    //  return
    //

    return new SegmentationRule(schemaAndValue, segmentationRuleCriteria, subscriberGroupName);
  }
  
  /*****************************************
  *
  *  unpackAutoTargetingCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackSegmentationRuleCriteria(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema evaluationCriterionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

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
  *  constructor -- JSON
  *
  *****************************************/

  public SegmentationRule(JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentationRuleUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSegmentationRuleUnchecked != null) ? existingSegmentationRuleUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSegmentationRule
    *
    *****************************************/

    SegmentationRule existingSegmentationRule = (existingSegmentationRuleUnchecked != null && existingSegmentationRuleUnchecked instanceof SegmentationRule) ? (SegmentationRule) existingSegmentationRuleUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.segmentationRuleCriteria = decodeSegmentationRuleCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "segmentationRuleCriteria", true));
    this.subscriberGroupName = JSONUtilities.decodeString(jsonRoot, "subscriberGroupName", true);
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSegmentationRule))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeSegmentationRuleCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeSegmentationRuleCriteria(JSONArray jsonArray) throws GUIManagerException
   {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
      }
    return result;
  }


  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SegmentationRule existingSegmentationRule)
  {
    if (existingSegmentationRule != null && existingSegmentationRule.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSegmentationRule.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(segmentationRuleCriteria, existingSegmentationRule.getSegmentationRuleCriteria());
        epochChanged = epochChanged || ! Objects.equals(subscriberGroupName, existingSegmentationRule.getSubscriberGroupName());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }
}
