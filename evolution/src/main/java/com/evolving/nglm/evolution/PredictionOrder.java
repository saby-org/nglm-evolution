/*****************************************************************************
*
*  PredictionOrder.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

public class PredictionOrder extends GUIManagedObject
{
  /*****************************************
  *
  * PredictionAlgorithm
  *
  *****************************************/
  public enum PredictionAlgorithm
  {
    ChurnPred("churn_pred"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PredictionAlgorithm(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PredictionAlgorithm fromExternalRepresentation(String externalRepresentation) { for (PredictionAlgorithm enumeratedValue : PredictionAlgorithm.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
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
    schemaBuilder.name("prediction_order");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), currentSchemaVersion));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("description",            Schema.STRING_SCHEMA);
    schemaBuilder.field("algorithm",              Schema.STRING_SCHEMA); // PredictionAlgorithm (unique string ID)
    schemaBuilder.field("annotation",             Schema.STRING_SCHEMA); // Field to be predicted 
    schemaBuilder.field("frequency",              Schema.STRING_SCHEMA); // TODO: change for JourneySchedule later ? 
    schemaBuilder.field("targetCriteria",         SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };
  
  private static ConnectSerde<PredictionOrder> serde = new ConnectSerde<PredictionOrder>(schema, false, PredictionOrder.class, PredictionOrder::pack, PredictionOrder::unpack);
  
  public static Schema schema() { return schema; }
  public static ConnectSerde<PredictionOrder> serde() { return serde; }
  
  /*****************************************
  *
  * Pack
  *
  *****************************************/
  public static Object pack(Object value)
  {
    PredictionOrder t = (PredictionOrder) value;
    
    Struct struct = new Struct(schema);
    packCommon(struct, t);
    struct.put("description", t.description);
    struct.put("algorithm", t.algorithm.getExternalRepresentation());
    struct.put("annotation", t.annotation);
    struct.put("frequency", t.frequency);
    struct.put("targetCriteria", t.targetCriteria.stream().map(EvaluationCriterion::pack).collect(Collectors.toList()));
    return struct;
  }
  
  /*****************************************
  *
  * Unpack
  *
  *****************************************/
  public static PredictionOrder unpack(SchemaAndValue schemaAndValue)
  {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //
    PredictionOrder result = new PredictionOrder(schemaAndValue);
    Struct valueStruct = (Struct) value;
    result.description = valueStruct.getString("description");
    result.algorithm = PredictionAlgorithm.fromExternalRepresentation(valueStruct.getString("algorithm"));
    result.annotation = valueStruct.getString("annotation");
    result.frequency = valueStruct.getString("frequency");
    result.targetCriteria = ((List<Object>) valueStruct.get("targetCriteria")).stream()
        .map(v -> EvaluationCriterion.unpack(new SchemaAndValue(schema, v))).collect(Collectors.toList());
    
    return result;
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private String description; 
  private PredictionAlgorithm algorithm;
  private String annotation;
  private String frequency;
  private List<EvaluationCriterion> targetCriteria;
  

  /*****************************************
  *
  * Accessors
  *
  *****************************************/
  public String getDescription(){ return description; }
  public PredictionAlgorithm getAlgorithm() { return algorithm; }
  public String getAnnotation() { return annotation; }
  public String getFrequency() { return frequency; }
  public List<EvaluationCriterion> getTargetCriteria() { return targetCriteria; }
  
  /*****************************************
  *
  * Constructor -- empty for unpack (private)
  *
  *****************************************/
  private PredictionOrder(SchemaAndValue schemaAndValue)
  {
    super(schemaAndValue);
  }

  /*****************************************
  *
  * Constructor -- from JSON for GUIManager (public)
  *
  *****************************************/
  public PredictionOrder(JSONObject jsonRoot, long epoch, GUIManagedObject existingPredictionOrderUnchecked, int tenantID) throws GUIManagerException, JSONUtilitiesException
  {
    super(jsonRoot, epoch, tenantID);

    /*****************************************
    *
    * Existing ?
    *
    *****************************************/
    PredictionOrder existingPredictionOrder = (existingPredictionOrderUnchecked != null && existingPredictionOrderUnchecked instanceof PredictionOrder) ? (PredictionOrder) existingPredictionOrderUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    this.description = JSONUtilities.decodeString(jsonRoot, "description", "");
    this.algorithm = PredictionAlgorithm.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "algorithm", true));
    this.annotation = JSONUtilities.decodeString(jsonRoot, "annotation", "");
    this.frequency = JSONUtilities.decodeString(jsonRoot, "frequency", true);
    this.targetCriteria = ((List<JSONObject>) JSONUtilities.decodeJSONArray(jsonRoot, "targetCriteria", true)).stream()
      .map(jsonObject -> {  try { return new EvaluationCriterion(jsonObject, CriterionContext.FullDynamicProfile(tenantID), tenantID); } catch (GUIManagerException e) { throw new JSONUtilitiesException(e.getMessage(), e.getCause()); } })
      .collect(Collectors.toList()); // We need to transform GUIManagerException into JSONUtilitiesException in lambda because GUIManagerException is not a RuntimeException
    // @rl: Not sure why FullDynamicProfile !
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/
    this.validate();

    /*****************************************
    *
    *  epoch
    *
    *****************************************/
    if (epochChanged(existingPredictionOrder))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  * validate
  *
  *****************************************/
  public void validate() throws GUIManagerException
  {
    if(algorithm.equals(PredictionAlgorithm.Unknown)) {
      throw new GUIManagerException("Unknown prediction alogirthm.", null);
    }
  }

  /*****************************************
  *
  * epochChanged
  *
  *****************************************/
  private boolean epochChanged(PredictionOrder predictionOrder)
  {
    if (predictionOrder != null && predictionOrder.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), predictionOrder.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(description, predictionOrder.getDescription());
        epochChanged = epochChanged || ! Objects.equals(algorithm, predictionOrder.getAlgorithm());
        epochChanged = epochChanged || ! Objects.equals(annotation, predictionOrder.getAnnotation());
        epochChanged = epochChanged || ! Objects.equals(frequency, predictionOrder.getFrequency());
        epochChanged = epochChanged || ! Objects.equals(targetCriteria, predictionOrder.getTargetCriteria());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
