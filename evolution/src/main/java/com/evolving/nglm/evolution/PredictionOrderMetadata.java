/*****************************************************************************
*
*  PredictionOrder.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberPredictions.Prediction;

import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * Metadata for a GUIManagedObject (PredictionOrder)
 * 
 * Those data will not be filled by GUI. Those are internal data that need to be shared 
 * with Spark, Evolution, (or display to the user in GUI, but in read-only mode). 
 * 
 * Therefore, we cannot store them in the GUIManagedObject because GUIManager (and thus GUI) 
 * is the MASTER of those data.
 * - GUIManager is the only one with the PredictionOrderService (GUIService) in master mode (write)
 * - Otherwise, if GUI modify a PredictionOrder, it will override any internal metadata not provided by the GUI
 *
 * Hence, those metadata are stored in auxiliary Object (this one) in a separate topic.
 * It will be retrieve in the PredictionOrderService through a ReferenceDataReader
 */
public class PredictionOrderMetadata implements ReferenceDataValue<String>
{
  /*****************************************
  *
  * Schema
  *
  *****************************************/
  private static Schema schema = null;
  private static int currentSchemaVersion = 1;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("prediction_order_metadata");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(currentSchemaVersion));
    schemaBuilder.field("predictionID",           Schema.STRING_SCHEMA);    // Ref ID to GUIManagedObject
    schemaBuilder.field("lastExecutionID",        Schema.INT32_SCHEMA);     // Updated by scheduler when all requests have been pushed in subscriberpredictionsrequest
    // TODO: ajouter les courbes ROC ici
    schema = schemaBuilder.build();
  };
  
  private static ConnectSerde<PredictionOrderMetadata> serde = new ConnectSerde<PredictionOrderMetadata>(schema, false, PredictionOrderMetadata.class, PredictionOrderMetadata::pack, PredictionOrderMetadata::unpack);
  
  public static Schema schema() { return schema; }
  public static ConnectSerde<PredictionOrderMetadata> serde() { return serde; }
  
  /*****************************************
  *
  * Pack
  *
  *****************************************/
  public static Object pack(Object value)
  {
    PredictionOrderMetadata t = (PredictionOrderMetadata) value;
    
    Struct struct = new Struct(schema);
    struct.put("predictionID", t.predictionID);
    struct.put("lastExecutionID", t.lastExecutionID);
    return struct;
  }
  
  /*****************************************
  *
  * Unpack
  *
  *****************************************/
  public static PredictionOrderMetadata unpack(SchemaAndValue schemaAndValue)
  {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //
    Struct valueStruct = (Struct) value;
    String predictionID = valueStruct.getString("predictionID");
    int lastExecutionID = valueStruct.getInt32("lastExecutionID");
    
    return new PredictionOrderMetadata(predictionID, lastExecutionID);
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private String predictionID;
  private int lastExecutionID;

  /*****************************************
  *
  * Accessors
  *
  *****************************************/
  public String getPredictionID(){ return predictionID; }
  public int getLastExecutionID() { return lastExecutionID; }
  
  /*****************************************
  *
  * ReferenceDataValue implementation
  *
  *****************************************/
  @Override public String getKey() { return predictionID; }

  /*****************************************
  *
  * Setters
  *
  *****************************************/
  public void incrementExecutionID() { lastExecutionID += 1; }
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  private PredictionOrderMetadata(String predictionID, int lastExecutionID) 
  {
    this.predictionID = predictionID;
    this.lastExecutionID = lastExecutionID;
  }
  
  /*****************************************
  *
  * Constructor - empty (for creation, when it does not exist in topic)
  *
  *****************************************/
  public PredictionOrderMetadata(String predictionID) 
  {
    this(predictionID, 0);
  }
}
