package com.evolving.nglm.evolution;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;

public class SubscriberPredictions
{
  /*****************************************
  *
  * Prediction
  * - Object pushed by prediction module in Evolution kafka topic subscriberpredictionspush
  * - Object stored in SubscriberProfile
  *
  *****************************************/
  public static class Prediction implements SubscriberStreamEvent
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
      schemaBuilder.field("subscriberID",  Schema.STRING_SCHEMA); // because SubscriberStreamEvent
      schemaBuilder.field("predictionID",  Schema.STRING_SCHEMA);
      schemaBuilder.field("score",         Schema.FLOAT64_SCHEMA);
      schemaBuilder.field("position",      Schema.FLOAT64_SCHEMA);
      schemaBuilder.field("date",          Schema.INT64_SCHEMA);
      return schemaBuilder.build();
    };
    
    private static ConnectSerde<Prediction> serde = new ConnectSerde<Prediction>(schema, false, Prediction.class, Prediction::pack, Prediction::unpack);
    public static Schema schema() { return schema; }
    public static ConnectSerde<Prediction> serde() { return serde; }
    
    /*****************************************
    *
    * Pack
    *
    *****************************************/
    public static Object pack(Object value)
    {
      Prediction prediction = (Prediction) value;
      
      Struct struct = new Struct(schema);
      struct.put("subscriberID", prediction.subscriberID);
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
      String subscriberID = valueStruct.getString("subscriberID");
      String predictionID = valueStruct.getString("predictionID");
      Double score = valueStruct.getFloat64("score");
      Double position = valueStruct.getFloat64("position");
      Long date = valueStruct.getInt64("date");
      
      return new Prediction(subscriberID, predictionID, score, position, new Date(date));
    }
    
    /*****************************************
    *
    * Properties
    *
    *****************************************/
    public String subscriberID;
    public String predictionID;
    public double score;
    public double position; // [0,1[ (p/PopSize)
    public Date date;
    
    public Prediction(String subscriberID, String predictionID, double score, double position, Date date) {
      this.subscriberID = subscriberID;
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
    public int getNcileInterval(Integer N) { 
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
    
    /*****************************************
    *
    * SubscriberStreamEvent
    *
    *****************************************/
    @Override public String getSubscriberID() { return this.subscriberID; }
    @Override public Date getEventDate() { return this.date; }
    @Override public Schema subscriberStreamEventSchema() { return schema; }
    @Override public Object subscriberStreamEventPack(Object value) { return pack(value); }
    @Override public DeliveryPriority getDeliveryPriority() { return DeliveryRequest.DeliveryPriority.High;} // @rl not sure ?
  }
  
  /*****************************************
  *
  * SubscriberPredictionsRequest
  * Object pushed by Evolution to be read by the prediction module
  *
  *****************************************/
  public static class SubscriberPredictionsRequest
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
      schemaBuilder.name("subscriber_predictions_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("subscriberID",  Schema.STRING_SCHEMA); // for consistency
      schemaBuilder.field("predictionID",  Schema.STRING_SCHEMA);
      schemaBuilder.field("executionID",   Schema.INT32_SCHEMA);
      schemaBuilder.field("trainingMode",  Schema.BOOLEAN_SCHEMA);
      return schemaBuilder.build();
    };  
    
    private static ConnectSerde<SubscriberPredictionsRequest> serde = new ConnectSerde<SubscriberPredictionsRequest>(schema, false, SubscriberPredictionsRequest.class, SubscriberPredictionsRequest::pack, SubscriberPredictionsRequest::unpack);
    public static Schema schema() { return schema; }
    public static ConnectSerde<SubscriberPredictionsRequest> serde() { return serde; }
    
    /*****************************************
    *
    * Pack
    *
    *****************************************/
    public static Object pack(Object value)
    {
      SubscriberPredictionsRequest t = (SubscriberPredictionsRequest) value;
      
      Struct struct = new Struct(schema);
      struct.put("subscriberID",  t.subscriberID);
      struct.put("predictionID",  t.predictionID);
      struct.put("executionID",   t.executionID);
      struct.put("trainingMode",  t.trainingMode);
      return struct;
    }
    
    /*****************************************
    *
    * Unpack
    *
    *****************************************/
    public static SubscriberPredictionsRequest unpack(SchemaAndValue schemaAndValue)
    {
      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      // unpack
      //
      Struct valueStruct = (Struct) value;
      String subscriberID = valueStruct.getString("subscriberID");
      String predictionID = valueStruct.getString("predictionID");
      int executionID = valueStruct.getInt32("executionID");
      boolean trainingMode = valueStruct.getBoolean("trainingMode");
      
      return new SubscriberPredictionsRequest(subscriberID, predictionID, executionID, trainingMode);
    }
    
    /*****************************************
    *
    * Properties
    *
    *****************************************/
    public String subscriberID;   // It is already the key of the topic. Only here for consistency (and having an independent object)
    public String predictionID;
    public int executionID;       // ExecutionID is here because we never clean manually the topic - (auto cleaned every 48h) 
                                  // It is also here for Spark to know that all requests has been pushed in the topic.
                                  // It is when PredictionSettingsMetadata.executionID switch to this one (at the very end of the push)
    public boolean trainingMode;  // isTraining ? (otherwise prediction mode)
    
    public SubscriberPredictionsRequest(String subscriberID, String predictionID, int executionID, boolean trainingMode) {
      this.subscriberID = subscriberID;
      this.predictionID = predictionID;
      this.executionID = executionID;
      this.trainingMode = trainingMode;
    }
  }
  
  /*****************************************
  *
  * SubscriberPrediction
  *
  *****************************************/
  
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
    result.current = ((List<Object>) valueStruct.get("currentPredictions")).stream()
        .map(v -> Prediction.unpack(new SchemaAndValue(schema.field("currentPredictions").schema().valueSchema(), v))).collect(Collectors.toMap(v -> v.predictionID, v -> v));
    result.previous = ((List<Object>) valueStruct.get("previousPredictions")).stream()
        .map(v -> Prediction.unpack(new SchemaAndValue(schema.field("previousPredictions").schema().valueSchema(), v))).collect(Collectors.toMap(v -> v.predictionID, v -> v));
    
    return result;
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private Map<String, Prediction> current;  // Key is PredictionID
  private Map<String, Prediction> previous; // Key is PredictionID

  /*****************************************
  *
  * Accessors
  *
  *****************************************/
  public Map<String, Prediction> getCurrent(){ return this.current; }
  public Map<String, Prediction> getPrevious(){ return this.previous; }
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
  
  /*****************************************
  *
  * Update
  *
  *****************************************/
  public void update(Prediction prediction) 
  {
    String predictionID = prediction.predictionID;
    if(this.current.get(predictionID) == null) {
      // New prediction 
      this.current.put(predictionID, prediction);
    }
    else {
      this.previous.put(predictionID, this.current.get(predictionID));
      this.current.put(predictionID, prediction);
    }
  }
}
