package com.evolving.nglm.evolution.otp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyObjectiveInstance;
import com.evolving.nglm.evolution.LoyaltyProgramState;
import com.evolving.nglm.evolution.ParameterMap;
import com.evolving.nglm.evolution.SubscriberRelatives;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class OTPType extends GUIManagedObject
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
    schemaBuilder.name("one_time_password_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    // TODO ajouter optionnal
    schemaBuilder.field("maxWrongCheckAttemptsByInstance", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("maxWrongCheckAttemptsByTimeWindow", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("maxConcurrentWithinTimeWindow", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("timeWindow", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("banPeriod", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("instanceExpirationDelay", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("valueGenerationDigits", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("valueGenerationRegex", Schema.OPTIONAL_STRING_SCHEMA);
    
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<OTPType> serde = new ConnectSerde<OTPType>(schema, false, OTPType.class, OTPType::pack, OTPType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<OTPType> serde() { return serde; }

/****************************************
  *
  *  data
  *
  ****************************************/

  // security attributes
  
  // Max number of time we can check this OTP with a wrong code before being considered as Expired...  (For One OTP Instance) 
  private Integer maxWrongCheckAttemptsByInstance;
  
  // 
  private Integer maxWrongCheckAttemptsByTimeWindow;
  
  // Max number of instances for which (creation date + time window) > now: Still valid in TimeWindow's perpective
  private Integer maxConcurrentWithinTimeWindow;
  
  // Duration during which an instance 
  private Integer timeWindow;
  
  private Integer banPeriod;  // int32 seconds allows >60years
 
  // affects instance values generation :
  private Integer instanceExpirationDelay;
  
  // only one of the two :
  private Integer valueGenerationDigits; // for simple non-0-leading number
  private String valueGenerationRegex; // for more complex type
  
  
  
  // possible future developments :
  // - add a flag so indicate if maxWrongCheckAttemptsByInstance raises a ban (currently yes) or just leave the instance maxed without additionnal error count (originally but dismissed)
  // - add a second timer or a flag to extend/reset the calculated expirationDate of the instance after a successfull check
  // ...
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //
  public String getOTPTypeID() { return getGUIManagedObjectID(); }
  public String getOTPTypeName() { return getGUIManagedObjectName(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  
  public Integer getMaxWrongCheckAttemptsByInstance() { return maxWrongCheckAttemptsByInstance; }
  public Integer getMaxWrongCheckAttemptsByTimeWindow() { return maxWrongCheckAttemptsByTimeWindow; }
  public Integer getMaxConcurrentWithinTimeWindow() { return maxConcurrentWithinTimeWindow; }
  public Integer getTimeWindow() { return timeWindow; }
  public Integer getBanPeriod() { return banPeriod; }
  public Integer getInstanceExpirationDelay() { return instanceExpirationDelay; }
  public Integer getValueGenerationDigits() { return valueGenerationDigits; }
  public String  getValueGenerationRegex() { return valueGenerationRegex; }

  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public OTPType(SchemaAndValue schemaAndValue, Integer maxWrongCheckAttemptsByInstance,
		  Integer maxWrongCheckAttemptsByTimeWindow, Integer maxConcurrentWithinTimeWindow, Integer timeWindow, Integer banPeriod,
		  Integer instanceExpirationDelay, Integer valueGenerationDigits, String valueGenerationRegex) {
	super(schemaAndValue);
	this.maxWrongCheckAttemptsByInstance = maxWrongCheckAttemptsByInstance;
	this.maxWrongCheckAttemptsByTimeWindow = maxWrongCheckAttemptsByTimeWindow;
	this.maxConcurrentWithinTimeWindow = maxConcurrentWithinTimeWindow;
	this.timeWindow = timeWindow;
	this.banPeriod = banPeriod;
	this.instanceExpirationDelay = instanceExpirationDelay;
	this.valueGenerationDigits = valueGenerationDigits;
	this.valueGenerationRegex = valueGenerationRegex;
  }

  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private OTPType(OTPType type)
  {
    super(type.getJSONRepresentation(), type.getEpoch(), type.getTenantID());
	this.maxWrongCheckAttemptsByInstance = type.maxWrongCheckAttemptsByInstance;
	this.maxWrongCheckAttemptsByTimeWindow = type.maxWrongCheckAttemptsByTimeWindow;
	this.maxConcurrentWithinTimeWindow = type.maxConcurrentWithinTimeWindow;
	this.timeWindow = type.timeWindow;
	this.banPeriod = type.banPeriod;
	this.instanceExpirationDelay = type.instanceExpirationDelay;
	this.valueGenerationDigits = type.valueGenerationDigits;
	this.valueGenerationRegex = type.valueGenerationRegex;
  }

  // GUI json creation
  public OTPType(JSONObject jsonRoot, long epoch, int tenantID)
  {
	super( jsonRoot, epoch, tenantID);  
	this.maxWrongCheckAttemptsByInstance =  JSONUtilities.decodeInteger(jsonRoot,"maxWrongCheckAttemptsByInstance", true);
	this.maxWrongCheckAttemptsByTimeWindow  =  JSONUtilities.decodeInteger(jsonRoot,"maxWrongCheckAttemptsByTimeWindow", true);
	this.maxConcurrentWithinTimeWindow  =  JSONUtilities.decodeInteger(jsonRoot,"maxConcurrentWithinTimeWindow", true);
	this.timeWindow  =  JSONUtilities.decodeInteger(jsonRoot,"timeWindow", true);
	this.banPeriod  =  JSONUtilities.decodeInteger(jsonRoot,"banPeriod", true);
	this.instanceExpirationDelay  =  JSONUtilities.decodeInteger(jsonRoot,"instanceExpirationDelay", true);
	this.valueGenerationDigits  =  JSONUtilities.decodeInteger(jsonRoot,"valueGenerationDigits", false);
	this.valueGenerationRegex  =  JSONUtilities.decodeString(jsonRoot,"valueGenerationRegex", false);
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public OTPType copy()
  {
    return new OTPType(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OTPType type = (OTPType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, type);
    struct.put("maxWrongCheckAttemptsByInstance", type.getMaxWrongCheckAttemptsByInstance());
    struct.put("maxWrongCheckAttemptsByTimeWindow", type.getMaxWrongCheckAttemptsByTimeWindow());
    struct.put("maxConcurrentWithinTimeWindow", type.getMaxConcurrentWithinTimeWindow());
    struct.put("timeWindow", type.getTimeWindow());
    struct.put("banPeriod", type.getBanPeriod());
    struct.put("instanceExpirationDelay", type.getInstanceExpirationDelay());
    struct.put("valueGenerationDigits", type.getValueGenerationDigits());
    struct.put("valueGenerationRegex", type.getValueGenerationRegex());
    return struct;
  }
    

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OTPType unpack(SchemaAndValue schemaAndValue)
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
    // passer en ternaire cast null explicite
    Integer maxWrongCheckAttemptsByInstance = (Integer) valueStruct.get("maxWrongCheckAttemptsByInstance");
    Integer maxWrongCheckAttemptsByTimeWindow = (Integer) valueStruct.get("maxWrongCheckAttemptsByTimeWindow");
    Integer maxConcurrentWithinTimeWindow = (Integer) valueStruct.get("maxConcurrentWithinTimeWindow");
    Integer timeWindow = (Integer) valueStruct.get("timeWindow");
    Integer banPeriod = (Integer) valueStruct.get("banPeriod");
    Integer instanceExpirationDelay = (Integer) valueStruct.get("instanceExpirationDelay");
    Integer valueGenerationDigits = (Integer) valueStruct.get("valueGenerationDigits");
    String valueGenerationRegex = (String) valueStruct.get("valueGenerationRegex");
        
    //
    //  return
    //

    return new OTPType(schemaAndValue, maxWrongCheckAttemptsByInstance,
    		 maxWrongCheckAttemptsByTimeWindow,  maxConcurrentWithinTimeWindow, timeWindow, banPeriod,
    		 instanceExpirationDelay,  valueGenerationDigits, valueGenerationRegex);
  }
  
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/
  
  public OTPType(JSONObject jsonRoot, long epoch, GUIManagedObject existingOTPTypeUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingOTPTypeUnchecked != null) ? existingOTPTypeUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingOTPTypeUnchecked
    *
    *****************************************/

    OTPType existingOTPType = (existingOTPTypeUnchecked != null && existingOTPTypeUnchecked instanceof OTPType) ? (OTPType) existingOTPTypeUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    // {
    //    "maxWrongCheckAttemptsByInstance" : 3,
    //    "maxWrongCheckAttemptsByTimeWindow" : 10,
    //    "maxConcurrentWithinTimeWindow" : 5,
    //    "timeWindowInSeconds" : 6000,
    //    "banPeriodInSeconds" : 10800,
    //    "instanceExpirationDelayInSeconds" : 600,
    //    "valueGenerationDigits" : 6,
    //    "valueGenerationRegex" : "[123456789][0123456789]{5}"
    //  }
    //      

    this.maxWrongCheckAttemptsByInstance  = JSONUtilities.decodeInteger(jsonRoot, "maxWrongCheckAttemptsByInstance", false); 
    this.maxWrongCheckAttemptsByTimeWindow  = JSONUtilities.decodeInteger(jsonRoot, "maxWrongCheckAttemptsByTimeWindow", false); 
    this.maxConcurrentWithinTimeWindow  = JSONUtilities.decodeInteger(jsonRoot, "maxConcurrentWithinTimeWindow", false); 
    this.timeWindow  = JSONUtilities.decodeInteger(jsonRoot, "timeWindowInSeconds", false); 
    this.banPeriod  = JSONUtilities.decodeInteger(jsonRoot, "banPeriodInSeconds", false); 
    this.instanceExpirationDelay  = JSONUtilities.decodeInteger(jsonRoot, "instanceExpirationDelayInSeconds", true); 
    this.valueGenerationDigits = JSONUtilities.decodeInteger(jsonRoot, "valueGenerationDigits", false);
    this.valueGenerationRegex = JSONUtilities.decodeString(jsonRoot, "valueGenerationRegex", false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingOTPType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(OTPType otpType)
  {
    if (otpType != null && otpType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), otpType.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getMaxWrongCheckAttemptsByInstance(), otpType.getMaxWrongCheckAttemptsByInstance());
        epochChanged = epochChanged || ! Objects.equals(getMaxWrongCheckAttemptsByTimeWindow(), otpType.getMaxWrongCheckAttemptsByTimeWindow());
        epochChanged = epochChanged || ! Objects.equals(getMaxConcurrentWithinTimeWindow(), otpType.getMaxConcurrentWithinTimeWindow());
        epochChanged = epochChanged || ! Objects.equals(getTimeWindow(), otpType.getTimeWindow());
        epochChanged = epochChanged || ! Objects.equals(getBanPeriod(), otpType.getBanPeriod());
        epochChanged = epochChanged || ! Objects.equals(getInstanceExpirationDelay(), otpType.getInstanceExpirationDelay());
        epochChanged = epochChanged || ! Objects.equals(getValueGenerationDigits(), otpType.getValueGenerationDigits());
        epochChanged = epochChanged || ! Objects.equals(getValueGenerationRegex(), otpType.getValueGenerationRegex());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }
}

