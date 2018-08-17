/*****************************************************************************
*
*  PresentationPosition.java
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

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

public class PresentationPosition
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
    schemaBuilder.name("presentation_position");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("offerTypes", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("additionalCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PresentationPosition> serde = new ConnectSerde<PresentationPosition>(schema, false, PresentationPosition.class, PresentationPosition::pack, PresentationPosition::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PresentationPosition> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Set<OfferType> offerTypes;
  private List<EvaluationCriterion> additionalCriteria;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<OfferType> getOfferTypes() { return offerTypes; }
  public List<EvaluationCriterion> getAdditionalCriteria() { return additionalCriteria; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationPosition(Set<OfferType> offerTypes, List<EvaluationCriterion> additionalCriteria)
  {
    this.offerTypes = offerTypes;
    this.additionalCriteria = additionalCriteria;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PresentationPosition presentationPosition = (PresentationPosition) value;
    Struct struct = new Struct(schema);
    struct.put("offerTypes", packOfferTypes(presentationPosition.getOfferTypes()));
    struct.put("additionalCriteria", packAdditionalCriteria(presentationPosition.getAdditionalCriteria()));
    return struct;
  }

  /****************************************
  *
  *  packOfferTypes
  *
  ****************************************/

  private static List<Object> packOfferTypes(Set<OfferType> offerTypes)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferType offerType : offerTypes)
      {
        result.add(offerType.getID());
      }
    return result;
  }

  /****************************************
  *
  *  packAdditionalCriteria
  *
  ****************************************/

  private static List<Object> packAdditionalCriteria(List<EvaluationCriterion> additionalCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : additionalCriteria)
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

  public static PresentationPosition unpack(SchemaAndValue schemaAndValue)
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
    Set<OfferType> offerTypes = unpackOfferTypes((List<String>) valueStruct.get("offerTypes"));
    List<EvaluationCriterion> additionalCriteria = unpackAdditionalCriteria(schema.field("additionalCriteria").schema(), valueStruct.get("additionalCriteria"));

    //
    //  return
    //

    return new PresentationPosition(offerTypes, additionalCriteria);
  }

  /*****************************************
  *
  *  unpackOfferTypes
  *
  *****************************************/

  private static Set<OfferType> unpackOfferTypes(List<String> offerTypeNames)
  {
    Set<OfferType> offerTypes = new HashSet<OfferType>();
    for (String offerTypeName : offerTypeNames)
      {
        OfferType offerType = Deployment.getOfferTypes().get(offerTypeName);
        if (offerType == null) throw new SerializationException("unknown offerType: " + offerTypeName);
        offerTypes.add(offerType);
      }
    return offerTypes;
  }
  
  /*****************************************
  *
  *  unpackAdditionalCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackAdditionalCriteria(Schema schema, Object value)
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
  *  constructor
  *
  *****************************************/

  public PresentationPosition(JSONObject jsonRoot) throws GUIManagerException
  {
    this.offerTypes = decodeOfferTypes(JSONUtilities.decodeJSONArray(jsonRoot, "offerTypeIDs", true));
    this.additionalCriteria = decodeAdditionalCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "additionalCriteria", true));
  }

  /*****************************************
  *
  *  decodeOfferTypes
  *
  *****************************************/

  private Set<OfferType> decodeOfferTypes(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferType> offerTypes = new HashSet<OfferType>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String offerTypeID = (String) jsonArray.get(i);
        OfferType offerType = Deployment.getOfferTypes().get(offerTypeID);
        if (offerType == null) throw new GUIManagerException("unknown offerType", offerTypeID);
        offerTypes.add(offerType);
      }
    return offerTypes;
  }

  /*****************************************
  *
  *  decodeAdditionalCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeAdditionalCriteria(JSONArray jsonArray) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Presentation));
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
    if (obj instanceof PresentationPosition)
      {
        PresentationPosition presentationPosition = (PresentationPosition) obj;
        result = true;
        result = result && Objects.equals(offerTypes, presentationPosition.getOfferTypes());
        result = result && Objects.equals(additionalCriteria, presentationPosition.getAdditionalCriteria());
      }
    return result;
  }
}
