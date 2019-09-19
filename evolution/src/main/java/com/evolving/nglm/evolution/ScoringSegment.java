/*****************************************************************************
 *
 * ScoringSegment.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

public class ScoringSegment
{
  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("scoring_segment");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("segmentIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("offerOptimizationAlgorithm", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameters", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("scoring_segment_parameters").schema());
    schemaBuilder.field("offerObjectiveIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("alwaysAppendOfferObjectiveIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ScoringSegment> serde = new ConnectSerde<ScoringSegment>(schema, false, ScoringSegment.class, ScoringSegment::pack, ScoringSegment::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ScoringSegment> serde() { return serde; }

  /*****************************************
   *
   * data
   *
   *****************************************/

  private Set<String> segmentIDs; 
  private OfferOptimizationAlgorithm offerOptimizationAlgorithm;
  private Map<OfferOptimizationAlgorithmParameter,String> parameters;
  private Set<String> offerObjectiveIDs; 
  private Set<String> alwaysAppendOfferObjectiveIDs; 

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public Set<String> getSegmentIDs() { return segmentIDs; }
  public OfferOptimizationAlgorithm getOfferOptimizationAlgorithm() { return offerOptimizationAlgorithm; }
  public Map<OfferOptimizationAlgorithmParameter,String> getParameters() { return parameters; }
  public Set<String> getOfferObjectiveIDs() { return offerObjectiveIDs; }
  public Set<String> getAlwaysAppendOfferObjectiveIDs() { return alwaysAppendOfferObjectiveIDs; }

  /*****************************************
   *
   * constructor
   *
   *****************************************/

  public ScoringSegment(Set<String> segmentIDs, OfferOptimizationAlgorithm offerOptimizationAlgorithm, Map<OfferOptimizationAlgorithmParameter,String> parameters, Set<String> offerObjectiveIDs, Set<String> alwaysAppendOfferObjectiveIDs)
    {
      this.segmentIDs = segmentIDs;
      this.offerOptimizationAlgorithm = offerOptimizationAlgorithm;
      this.parameters = parameters;
      this.offerObjectiveIDs = offerObjectiveIDs;
      this.alwaysAppendOfferObjectiveIDs = alwaysAppendOfferObjectiveIDs;
    }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
    {
      ScoringSegment scoringSegment = (ScoringSegment) value;
      Struct struct = new Struct(schema);
      struct.put("segmentIDs", packSegmentIDs(scoringSegment.getSegmentIDs()));
      struct.put("offerOptimizationAlgorithm", scoringSegment.getOfferOptimizationAlgorithm().getID());
      struct.put("parameters", packParameters(scoringSegment.getParameters()));
      struct.put("offerObjectiveIDs", packOfferObjectiveIDs(scoringSegment.getOfferObjectiveIDs()));
      struct.put("alwaysAppendOfferObjectiveIDs", packAlwaysAppendOfferObjectiveIDs(scoringSegment.getAlwaysAppendOfferObjectiveIDs()));
      return struct;
    }

  /****************************************
   *
   * packSegmentIDs
   *
   ****************************************/

  private static List<Object> packSegmentIDs(Set<String> segmentIDs)
    {
      List<Object> result = new ArrayList<Object>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }

  /****************************************
   *
   * packParameters
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
   * packOfferObjectiveIDs
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

  /****************************************
   *
   * packAlwaysAppendOfferObjectiveIDs
   *
   ****************************************/

  private static List<Object> packAlwaysAppendOfferObjectiveIDs(Set<String> alwaysAppendOfferObjectiveIDs)
    {
      List<Object> result = new ArrayList<Object>();
      for (String offerObjectiveID : alwaysAppendOfferObjectiveIDs)
        {
          result.add(offerObjectiveID);
        }
      return result;
    }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static ScoringSegment unpack(SchemaAndValue schemaAndValue)
    {
      //
      // data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      // unpack
      //

      Struct valueStruct = (Struct) value;
      Set<String> segmentIDs = unpackSegmentIDs((List<String>) valueStruct.get("segmentIDs"));
      OfferOptimizationAlgorithm offerOptimizationAlgorithm = Deployment.getOfferOptimizationAlgorithms().get(valueStruct.getString("offerOptimizationAlgorithm"));
      Map<OfferOptimizationAlgorithmParameter,String> parameters = unpackParameters((Map<String,String>) valueStruct.get("parameters"));
      Set<String> offerObjectiveIDs = unpackOfferObjectiveIDs((List<String>) valueStruct.get("offerObjectiveIDs"));
      Set<String> alwaysAppendOfferObjectiveIDs = unpackAlwaysAppendOfferObjectiveIDs((List<String>) valueStruct.get("alwaysAppendOfferObjectiveIDs"));
      //
      // validate
      //

      if (offerOptimizationAlgorithm == null) throw new SerializationException("unknown offerOptimizationAlgorithm: " + valueStruct.getString("offerOptimizationAlgorithm"));
      for (OfferOptimizationAlgorithmParameter parameter : parameters.keySet())
        {
          if (! offerOptimizationAlgorithm.getParameters().contains(parameter)) throw new SerializationException("unsupported offerOptimizationAlgorithmParameter: " + parameter.getParameterName());
        }

      //
      // return
      //

      return new ScoringSegment(segmentIDs, offerOptimizationAlgorithm, parameters, offerObjectiveIDs, alwaysAppendOfferObjectiveIDs);
    }

  /*****************************************
   *
   * unpackSegmentIDs
   *
   *****************************************/

  private static Set<String> unpackSegmentIDs(List<String> segmentIDs)
    {
      Set<String> result = new LinkedHashSet<String>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }

  /*****************************************
   *
   * unpackParameters
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
   * unpackOfferObjectiveIDs
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
   * unpackAlwaysAppendOfferObjectiveIDs
   *
   *****************************************/

  private static Set<String> unpackAlwaysAppendOfferObjectiveIDs(List<String> alwaysAppendOfferObjectiveIDs)
    {
      Set<String> result = new LinkedHashSet<String>();
      for (String offerObjectiveID : alwaysAppendOfferObjectiveIDs)
        {
          result.add(offerObjectiveID);
        }
      return result;
    }

  /*****************************************
   *
   * constructor
   *
   *****************************************/

  public ScoringSegment(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      // basic fields
      //

      this.segmentIDs = decodeSegmentIDs(JSONUtilities.decodeJSONArray(jsonRoot, "segmentIDs", false));
      String scoringAlgoId = JSONUtilities.decodeString(jsonRoot, "offerOptimizationAlgorithmID", true);
      if(scoringAlgoId.startsWith("DNBO"))
        {
          this.parameters = new HashMap<OfferOptimizationAlgorithmParameter,String>();
          this.offerOptimizationAlgorithm = Deployment.getOfferOptimizationAlgorithms().get("matrix-algorithm");
          this.parameters.put(new OfferOptimizationAlgorithmParameter("matrixID"),scoringAlgoId.replace("DNBO",""));
        }
      else
        {
          this.offerOptimizationAlgorithm = Deployment.getOfferOptimizationAlgorithms().get(scoringAlgoId);
          this.parameters = decodeParameters(JSONUtilities.decodeJSONObject(jsonRoot, "parameters", false));
        }

      this.offerObjectiveIDs = decodeOfferObjectiveIDs(JSONUtilities.decodeJSONArray(jsonRoot, "offerObjectiveIDs", true));
      this.alwaysAppendOfferObjectiveIDs = decodeAlwaysAppendOfferObjectiveIDs(JSONUtilities.decodeJSONArray(jsonRoot, "alwaysAppendOfferObjectiveIDs", false));

      //
      // validate 
      //

      if (this.offerOptimizationAlgorithm == null) throw new GUIManagerException("unsupported offerOptimizationAlgorithm", JSONUtilities.decodeString(jsonRoot, "offerOptimizationAlgorithmID", true));
      for (OfferOptimizationAlgorithmParameter parameter : this.parameters.keySet())
        {
          if (! offerOptimizationAlgorithm.getParameters().contains(parameter)) throw new GUIManagerException("unsupported offerOptimizationAlgorithmParameter", parameter.getParameterName());
        }
    }

  /*****************************************
   *
   * decodeSegmentIDs
   *
   *****************************************/

  private Set<String> decodeSegmentIDs(JSONArray jsonArray) throws GUIManagerException
    {
      Set<String> segmentIDs = new LinkedHashSet<String>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              String segmentID = (String) jsonArray.get(i);
              segmentIDs.add(segmentID);
            }
        }
      return segmentIDs;
    }

  /*****************************************
   *
   * decodeParameters
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
   * decodeOfferObjectiveIDs
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
   * decodeAlwaysAppendOfferObjectiveIDs
   *
   *****************************************/

  private Set<String> decodeAlwaysAppendOfferObjectiveIDs(JSONArray jsonArray) throws GUIManagerException
  {
    Set<String> alwaysAppendOfferObjectiveIDs = new LinkedHashSet<String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            String offerObjectiveID = (String) jsonArray.get(i);
            alwaysAppendOfferObjectiveIDs.add(offerObjectiveID);
          }
      }
    return alwaysAppendOfferObjectiveIDs;
  }

  /*****************************************
   *
   * equals
   *
   *****************************************/

  public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ScoringSegment)
        {
          ScoringSegment scoringSplit = (ScoringSegment) obj;
          result = true;
          result = result && Objects.equals(segmentIDs, scoringSplit.getSegmentIDs());
          result = result && Objects.equals(offerOptimizationAlgorithm, scoringSplit.getOfferOptimizationAlgorithm());
          result = result && Objects.equals(parameters, scoringSplit.getParameters());
          result = result && Objects.equals(offerObjectiveIDs, scoringSplit.getOfferObjectiveIDs());
          result = result && Objects.equals(alwaysAppendOfferObjectiveIDs, scoringSplit.getAlwaysAppendOfferObjectiveIDs());
        }
      return result;
    }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
    {
      String res = "";
      res = "ScoringSegment ["
          + (offerOptimizationAlgorithm != null
          ? "offerOptimizationAlgorithm=" + offerOptimizationAlgorithm + ", " : "")
          + (parameters != null ? "parameters=" + parameters + ", " : "");
      if (offerObjectiveIDs != null) 
        {
          res += "offerObjectiveIDs= [";
          boolean first = true;
          for (String s : offerObjectiveIDs) 
            {
              if (first) 
                {
                  res += s;
                  first = false;
                } 
              else 
                {
                  res += ", "+s;
                }
            }
          res += "]";
        }
      if (alwaysAppendOfferObjectiveIDs != null)
        {
          res += ", alwaysAppendOfferObjectiveIDs= [";
          boolean first = true;
          for (String s : alwaysAppendOfferObjectiveIDs) 
            {
              if (first) 
                {
                  res += s;
                  first = false;
                }
              else 
                {
                  res += ", "+s;
                }
            }
          res += "]";
        }
      res += "]";
      return res;
    }


}