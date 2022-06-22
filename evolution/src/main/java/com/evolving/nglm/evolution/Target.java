package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.Target.TargetStatus;

@GUIDependencyDef(objectType = "target", serviceClass = TargetService.class, dependencies = {"loyaltyProgramPoints", "loyaltyprogramchallenge", "loyaltyprogrammission"})
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

  TargetStatus getTargetStatus()
  {
	Date now = SystemTime.getCurrentTime();
    TargetStatus status = TargetStatus.Unknown;
    status = (status == TargetStatus.Unknown && !this.getAccepted()) ? TargetStatus.NotValid : status;
    status = (status == TargetStatus.Unknown && isActiveGUIManagedObject(this, now)) ? TargetStatus.Running : status;
    status = (status == TargetStatus.Unknown && this.getEffectiveEndDate().before(now)) ? TargetStatus.Complete : status;
    return status;
  }
  
  /*****************************************
  *
  *  isActiveGUIManagedObject
  *
  *****************************************/

  protected boolean isActiveGUIManagedObject(GUIManagedObject guiManagedObject, Date date) {
    if(guiManagedObject==null) return false;
    if(!guiManagedObject.getAccepted()) return false;
    if(guiManagedObject.getEffectiveStartDate().after(date)) return false;
    if(guiManagedObject.getEffectiveEndDate().before(date)) return false;
    return true;
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

  public Target(JSONObject jsonRoot, long epoch, GUIManagedObject existingTargetUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTargetUnchecked != null) ? existingTargetUnchecked.getEpoch() : epoch, tenantID);

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
              this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(segment0, "profileCriteria", true), new ArrayList<EvaluationCriterion>(), tenantID);
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

  private List<EvaluationCriterion> decodeCriteria(JSONArray jsonArray, List<EvaluationCriterion> universalCriteria, int tenantID) throws GUIManagerException
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
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.DynamicProfile(tenantID), tenantID));
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
  @Override 
  public Map<String, List<String>> getGUIDependencies(List<GUIService> guiServiceList, int tenantID)
  {
	  Map<String, List<String>> result = new HashMap<String, List<String>>();
	  List<String> loyaltyProgramPointsIDs = new ArrayList<String>();
	    List<String> loyaltyprogramchallengeIDs = new ArrayList<String>();
	    List<String> loyaltyprogrammissionIDs = new ArrayList<String>();
	    result.put("loyaltyprogrampoints", loyaltyProgramPointsIDs);
	    result.put("loyaltyprogramchallenge", loyaltyprogramchallengeIDs);
		result.put("loyaltyprogrammission", loyaltyprogrammissionIDs);
					for (EvaluationCriterion criterion : getTargetingCriteria()) {
				String loyaltyProgramPointsID = getGUIManagedObjectIDFromDynamicCriterion(criterion,
						"loyaltyprogrampoints", guiServiceList);
				String loyaltyprogramchallengeID = getGUIManagedObjectIDFromDynamicCriterion(criterion,
						"loyaltyprogramchallenge", guiServiceList);
				String loyaltyprogrammissionID = getGUIManagedObjectIDFromDynamicCriterion(criterion,
						"loyaltyprogrammission", guiServiceList);

				if (loyaltyProgramPointsID != null)
					loyaltyProgramPointsIDs.add(loyaltyProgramPointsID);
				if (loyaltyprogramchallengeID != null)
					loyaltyprogramchallengeIDs.add(loyaltyprogramchallengeID);
				if (loyaltyprogrammissionID != null) loyaltyprogrammissionIDs.add(loyaltyprogrammissionID);
	        
	       
	      }
	    
	  return result;
  }
  
  private String getGUIManagedObjectIDFromDynamicCriterion(EvaluationCriterion criteria, String objectType, List<GUIService> guiServiceList)
	{
		String result = null;
		LoyaltyProgramService loyaltyProgramService = null;
		String loyaltyProgramID = "";
		GUIManagedObject uncheckedLoyalty;
		try {
			Pattern fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
			Matcher fieldNameMatcher = fieldNamePattern.matcher(criteria.getCriterionField().getID());
			if (fieldNameMatcher.find()) {
				loyaltyProgramID = fieldNameMatcher.group(1);
				loyaltyProgramService = (LoyaltyProgramService) guiServiceList.stream()
						.filter(srvc -> srvc.getClass() == LoyaltyProgramService.class).findFirst().orElse(null);
			}
			if (loyaltyProgramService != null) {
				uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
				switch (objectType.toLowerCase()) {
				case "loyaltyprogrampoints":

					if (uncheckedLoyalty != null && uncheckedLoyalty.getAccepted()
							&& ((LoyaltyProgram) uncheckedLoyalty).getLoyaltyProgramType() == LoyaltyProgramType.POINTS)
						result = uncheckedLoyalty.getGUIManagedObjectID();

					break;

				case "loyaltyprogramchallenge":

					uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
					if (uncheckedLoyalty != null && uncheckedLoyalty.getAccepted()
							&& ((LoyaltyProgram) uncheckedLoyalty)
									.getLoyaltyProgramType() == LoyaltyProgramType.CHALLENGE)
						result = uncheckedLoyalty.getGUIManagedObjectID();

					break;

				case "loyaltyprogrammission":

					uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
					if (uncheckedLoyalty != null && uncheckedLoyalty.getAccepted()
							&& ((LoyaltyProgram) uncheckedLoyalty)
									.getLoyaltyProgramType() == LoyaltyProgramType.MISSION)
						result = uncheckedLoyalty.getGUIManagedObjectID();

					break;

				default:
					break;
				}
			}

		} catch (PatternSyntaxException e) {
			if (log.isTraceEnabled())
				log.trace("PatternSyntaxException Description: {}, Index: ", e.getDescription(), e.getIndex());
		}

		return result;
	}
}
