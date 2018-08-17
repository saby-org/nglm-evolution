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
    schemaBuilder.field("salesChannels", SchemaBuilder.array(Schema.STRING_SCHEMA));
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
  private Set<SalesChannel> salesChannels; 

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public OfferOptimizationAlgorithm getOfferOptimizationAlgorithm() { return offerOptimizationAlgorithm; }
  public Map<OfferOptimizationAlgorithmParameter,Integer> getParameters() { return parameters; }
  public Set<SalesChannel> getSalesChannels() { return salesChannels;  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringSplit(OfferOptimizationAlgorithm offerOptimizationAlgorithm, Map<OfferOptimizationAlgorithmParameter,Integer> parameters, Set<SalesChannel> salesChannels)
  {
    this.offerOptimizationAlgorithm = offerOptimizationAlgorithm;
    this.parameters = parameters;
    this.salesChannels = salesChannels;
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
    struct.put("salesChannels", packSalesChannels(scoringSplit.getSalesChannels()));
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
  *  packSalesChannels
  *
  ****************************************/

  private static List<Object> packSalesChannels(Set<SalesChannel> salesChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (SalesChannel salesChannel : salesChannels)
      {
        result.add(salesChannel.getID());
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
    Set<SalesChannel> salesChannels = unpackSalesChannels((List<String>) valueStruct.get("salesChannels"));

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

    return new ScoringSplit(offerOptimizationAlgorithm, parameters, salesChannels);
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
  *  unpackSalesChannels
  *
  *****************************************/

  private static Set<SalesChannel> unpackSalesChannels(List<String> salesChannelNames)
  {
    Set<SalesChannel> salesChannels = new LinkedHashSet<SalesChannel>();
    for (String salesChannelName : salesChannelNames)
      {
        SalesChannel salesChannel = Deployment.getSalesChannels().get(salesChannelName);
        if (salesChannel == null) throw new SerializationException("unknown salesChannel: " + salesChannelName);
        salesChannels.add(salesChannel);
      }
    return salesChannels;
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
    this.salesChannels = decodeSalesChannels(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelIDs", true));
    
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
  *  decodeSalesChannels
  *
  *****************************************/

  private Set<SalesChannel> decodeSalesChannels(JSONArray jsonArray) throws GUIManagerException
  {
    Set<SalesChannel> salesChannels = new LinkedHashSet<SalesChannel>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String salesChannelID = (String) jsonArray.get(i);
        SalesChannel salesChannel = Deployment.getSalesChannels().get(salesChannelID);
        if (salesChannel == null) throw new GUIManagerException("unknown salesChannel", salesChannelID);
        salesChannels.add(salesChannel);
      }
    return salesChannels;
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
        result = result && Objects.equals(salesChannels, scoringSplit.getSalesChannels());
      }
    return result;
  }
}
