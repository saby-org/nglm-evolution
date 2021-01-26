/*****************************************************************************
*
*  SegmentEligibility.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("dependentOnExtendedSubscriberProfile", SchemaBuilder.bool().defaultValue(false).schema());
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
  private boolean dependentOnExtendedSubscriberProfile;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private SegmentEligibility(String id, String name, List<EvaluationCriterion> profileCriteria, boolean dependentOnExtendedSubscriberProfile)
  {
    this.id = id;
    this.name = name;
    this.profileCriteria = profileCriteria;
    this.dependentOnExtendedSubscriberProfile = dependentOnExtendedSubscriberProfile;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  SegmentEligibility(JSONObject jsonRoot, int tenantID) throws GUIManagerException
  {
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);

    //
    //  profileCritera -- attempt to construct with standard SubscriberProfile
    //

    try
      {
        this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", new JSONArray()), CriterionContext.DynamicProfile(tenantID), tenantID);
        this.dependentOnExtendedSubscriberProfile = false;
      }
    catch (GUIManagerException e)
      {
        this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", new JSONArray()), CriterionContext.FullDynamicProfile(tenantID), tenantID);
        this.dependentOnExtendedSubscriberProfile = true;
      }
  }

  /*****************************************
  *
  *  decodeProfileCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeProfileCriteria(JSONArray jsonArray, CriterionContext context, int tenantID) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), context, tenantID)); 
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
  public boolean getDependentOnExtendedSubscriberProfile() { return dependentOnExtendedSubscriberProfile; }

  @Override public boolean getSegmentConditionEqual(Segment segment)
  {
    //verify if objects comapred are the same type
    if(segment.getClass().getTypeName() != this.getClass().getTypeName()) return false;
    //cast parameter to current type
    SegmentEligibility eligilitySegment = (SegmentEligibility)segment;
    //if profile criteria size is diferent than stored objects are not equals
    if(this.getProfileCriteria().size() != eligilitySegment.getProfileCriteria().size()) return false;
    //verify if each criteria exist and equal. At first element missing or not equal report inequality
    for(EvaluationCriterion criterion : this.getProfileCriteria())
    {
      //this is not follow code formating convention to be more readable.
      //the method equals from EvaluationCriterion cannot be used to get diff because contains Ojects.equals on types that are not override equals
      if(!eligilitySegment.getProfileCriteria().stream().anyMatch(p -> (p.getCriterionOperator().equals(criterion.getCriterionOperator())
                                                                    && (p.getArgumentExpression().equals(criterion.getArgumentExpression())))
                                                                    && (p.getCriterionField().equals(criterion.getCriterionField())))) return false;
    }
    return true;
  }

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
    struct.put("dependentOnExtendedSubscriberProfile", segment.getDependentOnExtendedSubscriberProfile());
    return struct;
  }

  /****************************************
  *
  *  packProfileCriteria
  *
  ****************************************/

  private static List<Object> packProfileCriteria(List<EvaluationCriterion> profileCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    profileCriteria = (profileCriteria != null) ? profileCriteria : Collections.<EvaluationCriterion>emptyList();
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

    Struct valueStruct = (Struct) value;
    String id = valueStruct.getString("id");
    String name = valueStruct.getString("name");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    boolean dependentOnExtendedSubscriberProfile = (schemaVersion >= 2) ? valueStruct.getBoolean("dependentOnExtendedSubscriberProfile") : false;
    
    //
    //  construct
    //

    SegmentEligibility result = new SegmentEligibility(id, name, profileCriteria, dependentOnExtendedSubscriberProfile);

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
