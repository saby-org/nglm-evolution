package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;
import com.evolving.nglm.evolution.Target.TargetStatus;

@GUIDependencyDef(objectType = "target", serviceClass = TargetService.class, dependencies = {})
public class Target extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  public enum TargetingType
  {
    File("FILE"),
    Eligibility("ELIGIBILITY"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private TargetingType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static TargetingType fromExternalRepresentation(String externalRepresentation) { for (TargetingType enumeratedValue : TargetingType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) { return enumeratedValue; } } return Unknown; }
  }
  
  //
  //  TargetStatus
  //

  public enum TargetStatus
  {
    NotValid("Not Valid"),
    Running("Running"),
    Complete("Complete"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private TargetStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static TargetStatus fromExternalRepresentation(String externalRepresentation) { for (TargetStatus enumeratedValue : TargetStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  /*****************************************
  *
  *  getTargetStatus
  *
  *****************************************/

  private TargetStatus getTargetStatus(GUIManagedObject guiManagedObject)
  {
	  System.out.println("inside get Target status");
    Date now = SystemTime.getCurrentTime();
    TargetStatus status = TargetStatus.Unknown;
    status = (status == TargetStatus.Unknown && !guiManagedObject.getAccepted()) ? TargetStatus.NotValid : status;
    status = (status == TargetStatus.Unknown && isActiveGUIManagedObject(guiManagedObject, now)) ? TargetStatus.Running : status;
    status = (status == TargetStatus.Unknown && guiManagedObject.getEffectiveEndDate().before(now)) ? TargetStatus.Complete : status;
 System.out.println("status is"+status);
    return status;
  }
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("targetName", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetingType", SchemaBuilder.string().defaultValue(TargetingType.File.getExternalRepresentation()).schema());   
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
  private TargetingType targetingType;
  private String targetFileID;
  private List<EvaluationCriterion> targetingCriteria;
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getTargetID() { return getGUIManagedObjectID(); }
  public String getTargetName() { return targetName; }
  public TargetingType getTargetingType() { return targetingType; }
  public String getTargetFileID () { return targetFileID; }
  public List<EvaluationCriterion> getTargetingCriteria() { return targetingCriteria; }
  
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Target(SchemaAndValue schemaAndValue, String targetName, TargetingType targetingType, String targetFileID, List<EvaluationCriterion> targetingCriteria)
  {
    super(schemaAndValue);
    this.targetName = targetName;
    this.targetingType = targetingType;
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
    struct.put("targetingType", target.getTargetingType().getExternalRepresentation());
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
    if (criteria != null){
	    for (EvaluationCriterion criterion : criteria)
	      {
	        result.add(EvaluationCriterion.pack(criterion));
	      }
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
    TargetingType targetingType = (schemaVersion >= 2) ? TargetingType.fromExternalRepresentation(valueStruct.getString("targetingType")) : TargetingType.File;
    List<EvaluationCriterion> targetingCriteria = unpackCriteria(schema.field("targetingCriteria").schema(), valueStruct.get("targetingCriteria"));
    
    //
    //  return
    //

    return new Target(schemaAndValue, targetName, targetingType, targetFileID, targetingCriteria);
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
    *  existingTarget
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
    this.targetingType = TargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true));

    /*****************************************
    *
    *  targeting criteria
    *
    *          // this is an adaptation of an object coming from the GUI with a wierd structure: TODO to be changed later for a simpler and more meamingfull structure
    *          //        "segments": [
    *          //                     {
    *          //                       "segId": "1",
    *          //                       "positionId": 0,
    *          //                       "name": "",
    *          //                       "profileCriteria": [
    *          //                         {
    *          //                           "argument": {
    *          //                             "valueAdd": null,
    *          //                             "expression": "55",
    *          //                             "valueMultiply": null,
    *          //                             "valueType": "simple",
    *          //                             "value": 55,
    *          //                             "timeUnit": null
    *          //                           },
    *          //                           "criterionField": "history.rechargeCount.previous7Days",
    *          //                           "criterionOperator": ">="
    *          //                         }
    *          //                       ]
    *          //                     }
    *          //                   ],
    *
    *****************************************/

    this.targetingCriteria = new ArrayList<EvaluationCriterion>();
    switch (this.targetingType)
      {
        case Eligibility:
          JSONArray segments = JSONUtilities.decodeJSONArray(jsonRoot, "segments", true);
          if (segments.size() > 0)
            {
              JSONObject segment0 = (JSONObject) segments.get(0);
              this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(segment0, "profileCriteria", true), new ArrayList<EvaluationCriterion>());
            }
          break;
      }

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
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.DynamicProfile));
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
        epochChanged = epochChanged || ! Objects.equals(targetingType, existingTarget.getTargetingType());
        epochChanged = epochChanged || ! Objects.equals(targetFileID, existingTarget.getTargetFileID());
        epochChanged = epochChanged || ! Objects.equals(targetingCriteria, existingTarget.getTargetingCriteria());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /****************************************
  *
  *  validate
  *
  ****************************************/
  
  public void validate(UploadedFileService uploadedFileService, Date now) throws GUIManagerException 
  {
    //
    //  ensure file exists if specified
    //

    if (targetFileID != null)
      {
        UploadedFile uploadedFile = uploadedFileService.getActiveUploadedFile(targetFileID, now);
        if (uploadedFile == null)
          { 
            throw new GUIManagerException("unknown uploaded file with id ", targetFileID);
          }
      }
  }
}
