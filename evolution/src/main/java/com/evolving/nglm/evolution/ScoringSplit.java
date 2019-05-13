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

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
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
    schemaBuilder.field("parameters", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("scoring_split_parameters").schema());
    schemaBuilder.field("offerObjectiveIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
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
  private Map<OfferOptimizationAlgorithmParameter,String> parameters;
  private Set<String> offerObjectiveIDs; 

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public OfferOptimizationAlgorithm getOfferOptimizationAlgorithm() { return offerOptimizationAlgorithm; }
  public Map<OfferOptimizationAlgorithmParameter,String> getParameters() { return parameters; }
  public Set<String> getOfferObjectiveIDs() { return offerObjectiveIDs;  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringSplit(OfferOptimizationAlgorithm offerOptimizationAlgorithm, Map<OfferOptimizationAlgorithmParameter,String> parameters, Set<String> offerObjectiveIDs)
  {
    this.offerOptimizationAlgorithm = offerOptimizationAlgorithm;
    this.parameters = parameters;
    this.offerObjectiveIDs = offerObjectiveIDs;
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
    struct.put("offerObjectiveIDs", packOfferObjectiveIDs(scoringSplit.getOfferObjectiveIDs()));
    return struct;
  }

  /****************************************
  *
  *  packParameters
  *
  ****************************************/

  private static Map<String,String> packParameters(Map<OfferOptimizationAlgorithmParameter,String> parameters)
  {
    Map<String,String> result = new LinkedHashMap<String,String>();
    for (OfferOptimizationAlgorithmParameter parameter : parameters.keySet())
      {
        String parameterValue = parameters.get(parameter);
        result.put(parameter.getParameterName(),parameterValue);
      }
    return result;
  }

  /****************************************
  *
  *  packOfferObjectiveIDs
  *
  ****************************************/

  private static List<Object> packOfferObjectiveIDs(Set<String> offerObjectiveIDs)
  {
    List<Object> result = new ArrayList<Object>();
    for (String offerObjectiveID : offerObjectiveIDs)
      {
        result.add(offerObjectiveID);
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
    Map<OfferOptimizationAlgorithmParameter,String> parameters = unpackParameters((Map<String,String>) valueStruct.get("parameters"));
    Set<String> offerObjectiveIDs = unpackOfferObjectiveIDs((List<String>) valueStruct.get("offerObjectiveIDs"));

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

    return new ScoringSplit(offerOptimizationAlgorithm, parameters, offerObjectiveIDs);
  }

  /*****************************************
  *
  *  unpackParameters
  *
  *****************************************/

  private static Map<OfferOptimizationAlgorithmParameter,String> unpackParameters(Map<String,String> parameters)
  {
    Map<OfferOptimizationAlgorithmParameter,String> result = new LinkedHashMap<OfferOptimizationAlgorithmParameter,String>();
    for (String parameterName : parameters.keySet())
      {
        String parameterValue = parameters.get(parameterName);
        result.put(new OfferOptimizationAlgorithmParameter(parameterName), parameterValue);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackOfferObjectiveIDs
  *
  *****************************************/

  private static Set<String> unpackOfferObjectiveIDs(List<String> offerObjectiveIDs)
  {
    Set<String> result = new LinkedHashSet<String>();
    for (String offerObjectiveID : offerObjectiveIDs)
      {
        result.add(offerObjectiveID);
      }
    return result;
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
    this.offerObjectiveIDs = decodeOfferObjectiveIDs(JSONUtilities.decodeJSONArray(jsonRoot, "offerObjectiveIDs", true));
    
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

  private Map<OfferOptimizationAlgorithmParameter,String> decodeParameters(JSONObject parametersRoot)
  {
    Map<OfferOptimizationAlgorithmParameter,String> result = new HashMap<OfferOptimizationAlgorithmParameter,String>();
    if (parametersRoot != null)
      {
        for (Object keyUnchecked : parametersRoot.keySet())
          {
            String key = (String) keyUnchecked;
            String value = (String) parametersRoot.get(key);
            result.put(new OfferOptimizationAlgorithmParameter(key),value);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeOfferObjectiveIDs
  *
  *****************************************/

  private Set<String> decodeOfferObjectiveIDs(JSONArray jsonArray) throws GUIManagerException
  {
    Set<String> offerObjectiveIDs = new LinkedHashSet<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String offerObjectiveID = (String) jsonArray.get(i);
        offerObjectiveIDs.add(offerObjectiveID);
      }
    return offerObjectiveIDs;
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
        result = result && Objects.equals(offerObjectiveIDs, scoringSplit.getOfferObjectiveIDs());
      }
    return result;
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String res = "";
    res = "ScoringSplit ["
        + (offerOptimizationAlgorithm != null
            ? "offerOptimizationAlgorithm=" + offerOptimizationAlgorithm + ", " : "")
        + (parameters != null ? "parameters=" + parameters + ", " : "");
    if (offerObjectiveIDs != null) {
      res += "offerObjectiveIDs= [";
      boolean first = true;
      for (String s : offerObjectiveIDs) {
        if (first) {
          res += s;
          first = false;
        } else {
          res += ", "+s;
        }
      }
      res += "]";
    }
    res += "]";
    return res;
  }
  
  
}
