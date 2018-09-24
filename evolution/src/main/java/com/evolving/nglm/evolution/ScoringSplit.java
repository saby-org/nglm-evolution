/*****************************************************************************
*
*  ScoringSplit.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

public class ScoringSplit
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
    schemaBuilder.name("scoring_split");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("offerOptimizationAlgorithm", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameters", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("scoring_split_parameters").schema());
    schemaBuilder.field("catalogObjectives", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ScoringSplit> serde = new ConnectSerde<ScoringSplit>(schema, false, ScoringSplit.class, ScoringSplit::pack, ScoringSplit::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ScoringSplit> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private OfferOptimizationAlgorithm offerOptimizationAlgorithm;
  private Map<OfferOptimizationAlgorithmParameter,Integer> parameters;
  private Set<CatalogObjective> catalogObjectives; 

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public OfferOptimizationAlgorithm getOfferOptimizationAlgorithm() { return offerOptimizationAlgorithm; }
  public Map<OfferOptimizationAlgorithmParameter,Integer> getParameters() { return parameters; }
  public Set<CatalogObjective> getCatalogObjectives() { return catalogObjectives;  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringSplit(OfferOptimizationAlgorithm offerOptimizationAlgorithm, Map<OfferOptimizationAlgorithmParameter,Integer> parameters, Set<CatalogObjective> catalogObjectives)
  {
    this.offerOptimizationAlgorithm = offerOptimizationAlgorithm;
    this.parameters = parameters;
    this.catalogObjectives = catalogObjectives;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ScoringSplit scoringSplit = (ScoringSplit) value;
    Struct struct = new Struct(schema);
    struct.put("offerOptimizationAlgorithm", scoringSplit.getOfferOptimizationAlgorithm().getID());
    struct.put("parameters", packParameters(scoringSplit.getParameters()));
    struct.put("catalogObjectives", packCatalogObjectives(scoringSplit.getCatalogObjectives()));
    return struct;
  }

  /****************************************
  *
  *  packParameters
  *
  ****************************************/

  private static Map<String,Integer> packParameters(Map<OfferOptimizationAlgorithmParameter,Integer> parameters)
  {
    Map<String,Integer> result = new LinkedHashMap<String,Integer>();
    for (OfferOptimizationAlgorithmParameter parameter : parameters.keySet())
      {
        Integer parameterValue = parameters.get(parameter);
        result.put(parameter.getParameterName(),parameterValue);
      }
    return result;
  }

  /****************************************
  *
  *  packCatalogObjectives
  *
  ****************************************/

  private static List<Object> packCatalogObjectives(Set<CatalogObjective> catalogObjectives)
  {
    List<Object> result = new ArrayList<Object>();
    for (CatalogObjective catalogObjective : catalogObjectives)
      {
        result.add(catalogObjective.getID());
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ScoringSplit unpack(SchemaAndValue schemaAndValue)
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
    OfferOptimizationAlgorithm offerOptimizationAlgorithm = Deployment.getOfferOptimizationAlgorithms().get(valueStruct.getString("offerOptimizationAlgorithm"));
    Map<OfferOptimizationAlgorithmParameter,Integer> parameters = unpackParameters((Map<String,Integer>) valueStruct.get("parameters"));
    Set<CatalogObjective> catalogObjectives = unpackCatalogObjectives((List<String>) valueStruct.get("catalogObjectives"));

    //
    //  validate
    //

    if (offerOptimizationAlgorithm == null) throw new SerializationException("unknown offerOptimizationAlgorithm: " + valueStruct.getString("offerOptimizationAlgorithm"));
    for (OfferOptimizationAlgorithmParameter parameter : parameters.keySet())
      {
        if (! offerOptimizationAlgorithm.getParameters().contains(parameter)) throw new SerializationException("unsupported offerOptimizationAlgorithmParameter: " + parameter.getParameterName());
      }
    
    //
    //  return
    //

    return new ScoringSplit(offerOptimizationAlgorithm, parameters, catalogObjectives);
  }

  /*****************************************
  *
  *  unpackParameters
  *
  *****************************************/

  private static Map<OfferOptimizationAlgorithmParameter,Integer> unpackParameters(Map<String,Integer> parameters)
  {
    Map<OfferOptimizationAlgorithmParameter,Integer> result = new LinkedHashMap<OfferOptimizationAlgorithmParameter,Integer>();
    for (String parameterName : parameters.keySet())
      {
        Integer parameterValue = parameters.get(parameterName);
        result.put(new OfferOptimizationAlgorithmParameter(parameterName), parameterValue);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackCatalogObjectives
  *
  *****************************************/

  private static Set<CatalogObjective> unpackCatalogObjectives(List<String> catalogObjectiveNames)
  {
    Set<CatalogObjective> catalogObjectives = new LinkedHashSet<CatalogObjective>();
    for (String catalogObjectiveName : catalogObjectiveNames)
      {
        CatalogObjective catalogObjective = Deployment.getCatalogObjectives().get(catalogObjectiveName);
        if (catalogObjective == null) throw new SerializationException("unknown catalogObjective: " + catalogObjectiveName);
        catalogObjectives.add(catalogObjective);
      }
    return catalogObjectives;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringSplit(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  basic fields
    //
    
    this.offerOptimizationAlgorithm = Deployment.getOfferOptimizationAlgorithms().get(JSONUtilities.decodeString(jsonRoot, "offerOptimizationAlgorithmID", true));
    this.parameters = decodeParameters(JSONUtilities.decodeJSONObject(jsonRoot, "parameters", false));
    this.catalogObjectives = decodeCatalogObjectives(JSONUtilities.decodeJSONArray(jsonRoot, "catalogObjectiveIDs", true));
    
    //
    //  validate 
    //
    
    if (this.offerOptimizationAlgorithm == null) throw new GUIManagerException("unsupported offerOptimizationAlgorithm", JSONUtilities.decodeString(jsonRoot, "offerOptimizationAlgorithmID", true));
    for (OfferOptimizationAlgorithmParameter parameter : this.parameters.keySet())
      {
        if (! offerOptimizationAlgorithm.getParameters().contains(parameter)) throw new GUIManagerException("unsupported offerOptimizationAlgorithmParameter", parameter.getParameterName());
      }
  }

  /*****************************************
  *
  *  decodeParameters
  *
  *****************************************/

  private Map<OfferOptimizationAlgorithmParameter,Integer> decodeParameters(JSONObject parametersRoot)
  {
    Map<OfferOptimizationAlgorithmParameter,Integer> result = new HashMap<OfferOptimizationAlgorithmParameter,Integer>();
    if (parametersRoot != null)
      {
        for (Object keyUnchecked : parametersRoot.keySet())
          {
            String key = (String) keyUnchecked;
            Integer value = new Integer(((Number) parametersRoot.get(key)).intValue());
            result.put(new OfferOptimizationAlgorithmParameter(key),value);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeCatalogObjectives
  *
  *****************************************/

  private Set<CatalogObjective> decodeCatalogObjectives(JSONArray jsonArray) throws GUIManagerException
  {
    Set<CatalogObjective> catalogObjectives = new LinkedHashSet<CatalogObjective>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String catalogObjectiveID = (String) jsonArray.get(i);
        CatalogObjective catalogObjective = Deployment.getCatalogObjectives().get(catalogObjectiveID);
        if (catalogObjective == null) throw new GUIManagerException("unknown catalogObjective", catalogObjectiveID);
        catalogObjectives.add(catalogObjective);
      }
    return catalogObjectives;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ScoringSplit)
      {
        ScoringSplit scoringSplit = (ScoringSplit) obj;
        result = true;
        result = result && Objects.equals(offerOptimizationAlgorithm, scoringSplit.getOfferOptimizationAlgorithm());
        result = result && Objects.equals(parameters, scoringSplit.getParameters());
        result = result && Objects.equals(catalogObjectives, scoringSplit.getCatalogObjectives());
      }
    return result;
  }
}
