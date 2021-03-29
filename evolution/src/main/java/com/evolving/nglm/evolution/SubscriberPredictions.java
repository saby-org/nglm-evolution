package com.evolving.nglm.evolution;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class SubscriberPredictions
{
  /*****************************************
  *
  * Prediction
  *
  *****************************************/
  public static class Prediction 
  {
    /*****************************************
    *
    * Schema
    *
    *****************************************/
    public static final Schema schema = buildSchema();
    private static Schema buildSchema()
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("prediction");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("predictionID",  Schema.INT64_SCHEMA);
      schemaBuilder.field("score",         Schema.FLOAT64_SCHEMA);
      schemaBuilder.field("position",      Schema.FLOAT64_SCHEMA);
      schemaBuilder.field("date",          Schema.INT64_SCHEMA);
      return schemaBuilder.build();
    };
    
    /*****************************************
    *
    * Pack
    *
    *****************************************/
    public static Object pack(Object value)
    {
      Prediction prediction = (Prediction) value;
      
      Struct struct = new Struct(schema);
      struct.put("predictionID", prediction.predictionID);
      struct.put("score", prediction.score);
      struct.put("position", prediction.position);
      struct.put("date", prediction.date.getTime());
      return struct;
    }
    
    /*****************************************
    *
    * Unpack
    *
    *****************************************/
    public static Prediction unpack(SchemaAndValue schemaAndValue)
    {
      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      // unpack
      //
      Struct valueStruct = (Struct) value;
      Long predictionID = valueStruct.getInt64("predictionID");
      Double score = valueStruct.getFloat64("score");
      Double position = valueStruct.getFloat64("position");
      Long date = valueStruct.getInt64("date");
      
      return new Prediction(predictionID, score, position, new Date(date));
    }
    
    /*****************************************
    *
    * Properties
    *
    *****************************************/
    public long predictionID;
    public double score;
    public double position; // [0,1[ (p/PopSize)
    public Date date;
    
    public Prediction(long predictionID, double score, double position, Date date) {
      this.predictionID = predictionID;
      this.score = score; 
      this.position = position;
      this.date = date;
    }
    
    
    /**
     * @return t in [1,N] the interval between the (t–1} N-cile and the t N-cile.
     * Friendly reminder: there is N-1 N-ciles (values) and N N-cile intervals (group of values).
     * Here we are returning the N-cile interval of the prediction score.
     */
    private int getNcileInterval(Integer N) { 
      return ((int) (position * N)) + 1; // Casting to an int implicitly drops any decimal. No need to call Math.floor()
    }

    
    /**
     * @return t in [1,10] the interval between the (t–1} decile and the t decile.
     * Friendly reminder: there is 9 deciles (values) and 10 decile intervals (group of values).
     * Here we are returning the decile interval of the prediction score.
     */
    public int getDecileInterval() { 
      return getNcileInterval(10);
    }
  }
  
  /*****************************************
  *
  * Schema
  *
  *****************************************/
  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("subscriber_predictions");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("currentPredictions", SchemaBuilder.array(Prediction.schema).schema());
    schemaBuilder.field("previousPredictions", SchemaBuilder.array(Prediction.schema).schema());
    schema = schemaBuilder.build();
  };
  
  private static ConnectSerde<SubscriberPredictions> serde = new ConnectSerde<SubscriberPredictions>(schema, false, SubscriberPredictions.class, SubscriberPredictions::pack, SubscriberPredictions::unpack);
  
  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberPredictions> serde() { return serde; }
  
  /*****************************************
  *
  * Pack
  *
  *****************************************/
  public static Object pack(Object value)
  {
    SubscriberPredictions subscriberPredictions = (SubscriberPredictions) value;
    
    Struct struct = new Struct(schema);
    struct.put("currentPredictions", subscriberPredictions.current.values().stream().map(Prediction::pack).collect(Collectors.toList()));
    struct.put("previousPredictions", subscriberPredictions.previous.values().stream().map(Prediction::pack).collect(Collectors.toList()));
    return struct;
  }
  
  /*****************************************
  *
  * Unpack
  *
  *****************************************/
  public static SubscriberPredictions unpack(SchemaAndValue schemaAndValue)
  {
    SubscriberPredictions result = new SubscriberPredictions();
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //
    Struct valueStruct = (Struct) value;
    List<Prediction> currentArray = ((List<Object>) valueStruct.get("currentPredictions")).stream()
        .map(v -> Prediction.unpack(new SchemaAndValue(schema, v))).collect(Collectors.toList());
    List<Prediction> previousArray = ((List<Object>) valueStruct.get("previousPredictions")).stream()
        .map(v -> Prediction.unpack(new SchemaAndValue(schema, v))).collect(Collectors.toList());
    
    //
    // build
    //
    for(Prediction prediction : currentArray) {
      result.current.put(prediction.predictionID, prediction);
    }
    for(Prediction prediction : previousArray) {
      result.previous.put(prediction.predictionID, prediction);
    }
    
    return result;
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private Map<Long, Prediction> current;  // Key is PredictionID
  private Map<Long, Prediction> previous; // Key is PredictionID

  /*****************************************
  *
  * Accessors
  *
  *****************************************/
  public Map<Long, Prediction> getCurrent(){ return this.current; }
  public Map<Long, Prediction> getPrevious(){ return this.previous; }
  public Double getPredictionScore(Long predictionID) { return (current.get(predictionID) != null) ? current.get(predictionID).score : null; }
  public Integer getPredictionDecile(Long predictionID) { return (current.get(predictionID) != null) ? current.get(predictionID).getDecileInterval() : null; }

  /*****************************************
  *
  * Constructor -- simple: empty
  *
  *****************************************/
  public SubscriberPredictions()
  {
    this.current = new HashMap<>();
    this.previous = new HashMap<>();
  }
  
  /*****************************************
  *
  * Constructor -- shallow copy
  *
  *****************************************/
  public SubscriberPredictions(SubscriberPredictions predictions)
  {
    this.current = new HashMap<>(predictions.current);
    this.previous = new HashMap<>(predictions.previous);
  }
}
