/*****************************************************************************
*
*  CriteriaStory.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CriteriaStory extends GUIManagedObject
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
    schemaBuilder.name("criteria_story");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("story", Schema.STRING_SCHEMA);
    schemaBuilder.field("storyContext", Schema.STRING_SCHEMA);
    schemaBuilder.field("storyCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CriteriaStory> serde = new ConnectSerde<CriteriaStory>(schema, false, CriteriaStory.class, CriteriaStory::pack, CriteriaStory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CriteriaStory> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private CriterionContext criterionContext;
  private String story;
  private CriterionContext storyContext;
  private List<EvaluationCriterion> storyCriteria;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getCriteriaStoryID() { return getGUIManagedObjectID(); }
  public String getStory() { return story; }
  public CriterionContext getStoryContext() { return storyContext; }
  public List<EvaluationCriterion> getStoryCriteria() { return storyCriteria; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CriteriaStory(SchemaAndValue schemaAndValue, String story, CriterionContext storyContext, List<EvaluationCriterion> storyCriteria)
  {
    super(schemaAndValue);
    this.story = story;
    this.storyContext = storyContext;
    this.storyCriteria = storyCriteria;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CriteriaStory criteriaStory = (CriteriaStory) value;
    Struct struct = new Struct(schema);
    packCommon(struct, criteriaStory);
    struct.put("story", criteriaStory.getStory());
    struct.put("storyContext", criteriaStory.getStoryContext().getExternalRepresentation());
    struct.put("storyCriteria", packStoryCriteria(criteriaStory.getStoryCriteria()));
    return struct;
  }

  /****************************************
  *
  *  packStoryCriteria
  *
  ****************************************/

  private static List<Object> packStoryCriteria(List<EvaluationCriterion> storyCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : storyCriteria)
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

  public static CriteriaStory unpack(SchemaAndValue schemaAndValue)
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
    String story = valueStruct.getString("story");
    CriterionContext storyContext = CriterionContext.fromExternalRepresentation(valueStruct.getString("storyContext"));
    List<EvaluationCriterion> storyCriteria = unpackStoryCriteria(schema.field("storyCriteria").schema(), valueStruct.get("storyCriteria"));

    //
    //  return
    //

    return new CriteriaStory(schemaAndValue, story, storyContext, storyCriteria);
  }
  
  /*****************************************
  *
  *  unpackStoryCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackStoryCriteria(Schema schema, Object value)
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

  public CriteriaStory(JSONObject jsonRoot, long epoch, GUIManagedObject existingCriteriaStoryUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, "criteriaStoryID", (existingCriteriaStoryUnchecked != null) ? existingCriteriaStoryUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCriteriaStory
    *
    *****************************************/

    CriteriaStory existingCriteriaStory = (existingCriteriaStoryUnchecked != null && existingCriteriaStoryUnchecked instanceof CriteriaStory) ? (CriteriaStory) existingCriteriaStoryUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.story = JSONUtilities.decodeString(jsonRoot, "story", true);
    this.storyContext = CriterionContext.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "storyContext", true));
    this.storyCriteria = decodeStoryCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "storyCriteria", true), this.storyContext);

    /*****************************************
    *
    *  validate start/end dates
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  validate story against storyCriteria
    *
    *****************************************/

    //
    //  storyReferences
    //

    Set<String> storyReferences = new HashSet<String>();
    for (EvaluationCriterion storyCriterion : this.storyCriteria)
      {
        storyReferences.add(storyCriterion.getStoryReference());
      }

    //
    //  story and criteria
    //

    Pattern pattern = Pattern.compile("@(.*?)@");
    Matcher matcher = pattern.matcher(this.story);
    while (matcher.find())
      {
        String storyReference = matcher.group(1);
        if (! storyReferences.contains(storyReference))
          {
            throw new GUIManagerException("unsupported storyReference", storyReference);
          }
      }

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCriteriaStory))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeStoryCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeStoryCriteria(JSONArray jsonArray, CriterionContext storyContext) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), storyContext));
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CriteriaStory existingCriteriaStory)
  {
    if (existingCriteriaStory != null && existingCriteriaStory.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getName(), existingCriteriaStory.getName());
        epochChanged = epochChanged || ! Objects.equals(story, existingCriteriaStory.getStory());
        epochChanged = epochChanged || ! Objects.equals(storyContext, existingCriteriaStory.getStoryContext());
        epochChanged = epochChanged || ! Objects.equals(storyCriteria, existingCriteriaStory.getStoryCriteria());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
