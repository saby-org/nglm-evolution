package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class Target extends GUIManagedObject
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
    schemaBuilder.name("target");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("targetName", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetFileID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("targetingCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).optional().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Target> serde = new ConnectSerde<Target>(schema, false, Target.class, Target::pack, Target::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Target> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String targetName;
  private String targetFileID;
  private List<EvaluationCriterion> targetingCriteria;
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getTargetID() { return getGUIManagedObjectID(); }
  public String getTargetName() { return targetName; }
  public String getTargetFileID () { return targetFileID; }
  public List<EvaluationCriterion> getTargetingCriteria() { return targetingCriteria; }
  
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Target(SchemaAndValue schemaAndValue, String targetName, String targetFileID, List<EvaluationCriterion> targetingCriteria)
  {
    super(schemaAndValue);
    this.targetName = targetName;
    this.targetFileID = targetFileID;
    this.targetingCriteria = targetingCriteria;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Target target = (Target) value;
    Struct struct = new Struct(schema);
    packCommon(struct, target);
    struct.put("targetName", target.getTargetName());
    struct.put("targetFileID", target.getTargetFileID());
    struct.put("targetingCriteria", packCriteria(target.getTargetingCriteria()));
    return struct;
  }
  
  /****************************************
  *
  *  packCriteria
  *
  ****************************************/

  private static List<Object> packCriteria(List<EvaluationCriterion> criteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : criteria)
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

  public static Target unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    
    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String targetName = valueStruct.getString("targetName");
    String targetFileID = valueStruct.getString("targetFileID");
    List<EvaluationCriterion> targetingCriteria = unpackCriteria(schema.field("targetingCriteria").schema(), valueStruct.get("targetingCriteria"));
    
    //
    //  return
    //

    return new Target(schemaAndValue, targetName, targetFileID, targetingCriteria);
  }
  
  /*****************************************
  *
  *  unpackCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackCriteria(Schema schema, Object value)
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

  public Target(JSONObject jsonRoot, long epoch, GUIManagedObject existingTargetUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTargetUnchecked != null) ? existingTargetUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSupplier
    *
    *****************************************/

    Target existingTarget = (existingTargetUnchecked != null && existingTargetUnchecked instanceof Target) ? (Target) existingTargetUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.targetName = JSONUtilities.decodeString(jsonRoot, "targetName", true);
    this.targetFileID = JSONUtilities.decodeString(jsonRoot, "targetFileID", false);
    this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetingCriteria", false), new ArrayList<EvaluationCriterion>());

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingTarget))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeCriteria(JSONArray jsonArray, List<EvaluationCriterion> universalCriteria) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();

    //
    //  universal criteria
    //

    result.addAll(universalCriteria);

    //
    //  targeting critera
    //

    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
          }
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Target existingTarget)
  {
    if (existingTarget != null && existingTarget.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingTarget.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(targetName, existingTarget.getTargetName());
        epochChanged = epochChanged || ! Objects.equals(targetFileID, existingTarget.getTargetFileID());
        epochChanged = epochChanged || ! Objects.equals(targetingCriteria, existingTarget.getTargetingCriteria());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  public void validate(UploadedFileService uploadedFileService, Date now) throws GUIManagerException 
  {
    if(this.targetFileID != null) {
      UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(targetFileID);
      if (uploadedFile == null) throw new GUIManagerException("unknown uploaded file with id ", targetFileID);
    }
  }
}
