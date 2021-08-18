/****************************************************************************
*
*  ComplexObjectInstances.java
*
****************************************************************************/

package com.evolving.nglm.evolution.otp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Token;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectTypeService;
import com.evolving.nglm.evolution.datamodel.DataModelFieldValue;

public class OTPInstance // implements Action
{
	  //
	  //  logger
	  //

	  private static final Logger log = LoggerFactory.getLogger(OTPInstance.class);

	  /*****************************************
	  *
	  *  enum
	  *
	  *****************************************/

	  public enum OTPStatus
	  {
	    New("new"), // just generated, no check ever
	    Expired("expired"), // ovewriten by a newer instance generation or flagged expired by purge mechanism
	    ChecksSuccess("checked"), // latest call was a success but OTP is still valid/still allows checks
	    ChecksError("error"), // latest call was an error/failure but OTP is still valid/still allows checks
	    Burnt("burnt"), // there were a successful check with burn=true that deactivated the instance
	    MaxNbReached("maxnbreached"), // too many check errors on this one.
	    RaisedBan("raisedban"), // ban was raised due to too many errors global or too many generates.
	    Unknown("(unknown)");  // you don't want this one
	    private String externalRepresentation;
	    private OTPStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
	    public String getExternalRepresentation() { return externalRepresentation; }
	    public static OTPStatus fromExternalRepresentation(String externalRepresentation) { for (OTPStatus enumeratedValue : OTPStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
	  }

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  private static Schema schema = null;
  
  private static OTPTypeService otpTypeService;
  static
  {
	  otpTypeService = new OTPTypeService(System.getProperty("broker.servers"), "otpinstance-otptypeservice", Deployment.getOTPTypeTopic(), false);
	  otpTypeService.start();
        
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    // properties inherited from parent
    schemaBuilder.name("one_time_password_instance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    // otp properties
    schemaBuilder.field("otpType", Schema.STRING_SCHEMA);
    schemaBuilder.field("otpStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("otpValue", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("checksCount", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("errorCount", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("creationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("latestUpdate", Timestamp.builder().optional().schema());
    schemaBuilder.field("latestSuccess", Timestamp.builder().optional().schema());
    schemaBuilder.field("latestError", Timestamp.builder().optional().schema());
    schemaBuilder.field("expirationDate", Timestamp.builder().optional().schema());
    
    schema = schemaBuilder.build();
  };
  
  
  //
  // serde
  //

  private static ConnectSerde<OTPInstance> serde = new ConnectSerde<OTPInstance>(schema, false, OTPInstance.class, OTPInstance::pack, OTPInstance::unpack);
  
  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<OTPInstance> serde() { return serde; }

  /*****************************************
  *
  * data
  *
  *****************************************/

  private String otpTypeDisplayName;
  private OTPStatus otpStatus;
  private String otpValue;
  private Integer checksCount;
  private Integer errorCount;
  private Date creationDate;
  private Date latestUpdate;
  private Date latestSuccess;
  private Date latestError;
  private Date expirationDate;
   
  //private String elementID;
  //private Map<String, DataModelFieldValue> fieldValues; // key is the fieldName

  /*****************************************
  *
  * accessors
  *
  *****************************************/

  public String getOTPTypeDisplayName()    {		return otpTypeDisplayName;	}
  public OTPStatus getOTPStatus() {		return otpStatus;	}
  public String getOTPValue()     {		return otpValue;	}
  public Integer getChecksCount() {		return checksCount;	}
  public Integer getErrorCount()  {		return errorCount;	}
  public Date getCreationDate()   {		return creationDate;	}
  public Date getLatestUpdate()   {		return latestUpdate;	}
  public Date getLatestSuccess()   {		return latestSuccess;	}
  public Date getLatestError()   {		return latestError;	}
  public Date getExpirationDate() {		return expirationDate;	}
  
//  // Action implementation (matches enum in ActionManager ...)
//  
//  public ActionType getActionType() { return ActionType.OTPChange; }
  
  //
  //  setters
  //

  public void setOTPTypeDisplayName(String otpTypeDisplayName)    {		this.otpTypeDisplayName = otpTypeDisplayName;	}
  public void setOTPStatus(OTPStatus otpStatus) {		this.otpStatus = otpStatus;	}
  public void setOTPValue(String otpValue)      {		this.otpValue = otpValue;	}
  public void setChecksCount(Integer checksCount) {		this.checksCount = checksCount;	}
  public void setErrorCount(Integer errorCount)  {		this.errorCount = errorCount;	}
  public void setCreationDate(Date creationDate) {		this.creationDate = creationDate;	}
  public void setLatestUpdate(Date latestUpdate) {		this.latestUpdate = latestUpdate;	}
  public void setLatestSuccess(Date latestSuccess)   {		this.latestSuccess = latestSuccess;	}
  public void setLatestError(Date latestError)  {		this.latestError = latestError;	}
  public void setExpirationDate(Date expirationDate) {		this.expirationDate = expirationDate;	}
  
  /*****************************************
  *
  * constructor default
  *
  *****************************************/

  public OTPInstance(String otpTypeDisplayName, OTPStatus otpStatus,String otpValue,Integer checksCount,Integer errorCount,
		  Date creationDate,Date latestUpdate,Date latestSuccess, Date latestError, Date expirationDate)
  {
	 this.otpTypeDisplayName = otpTypeDisplayName;
	 this.otpStatus = otpStatus;
	 this.otpValue = otpValue;
	 this.checksCount = checksCount;
	 this.errorCount = errorCount;
	 this.creationDate = creationDate;
	 this.latestUpdate = latestUpdate;
	 this.latestSuccess = latestSuccess;
	 this.latestError = latestError;
	 this.expirationDate = expirationDate;
  }
  
  
  // may be usefull for GUI future developments
  public OTPInstance(JSONObject jsonRoot, long epoch, int tenantID)
  {
	// name, id ?? super( jsonRoot, epoch, tenantID);
	this.otpTypeDisplayName =  JSONUtilities.decodeString(jsonRoot,"otpTypeDisplayName", true);
	this.otpStatus  =  OTPStatus.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot,"otpStatus", true));
	this.otpValue  =  JSONUtilities.decodeString(jsonRoot,"otpValue", true);
	this.checksCount  =  JSONUtilities.decodeInteger(jsonRoot,"checksCount", true);
	this.errorCount  =  JSONUtilities.decodeInteger(jsonRoot,"errorCount", true);
	this.creationDate  =  JSONUtilities.decodeDate(jsonRoot,"creationDate", true);
	this.latestUpdate  =  JSONUtilities.decodeDate(jsonRoot,"latestUpdate", false);
	this.latestSuccess  =  JSONUtilities.decodeDate(jsonRoot,"latestSuccess", false);
	this.latestError = JSONUtilities.decodeDate(jsonRoot,"latestError", false);
	this.expirationDate = JSONUtilities.decodeDate(jsonRoot,"expirationDate", true);
  }
 
  
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OTPInstance otpInstance = (OTPInstance) value;
    Struct struct = new Struct(schema);
    struct.put("otpType", otpInstance.getOTPTypeDisplayName());
    struct.put("otpStatus", otpInstance.getOTPStatus().getExternalRepresentation());
    struct.put("otpValue", otpInstance.getOTPValue());
    struct.put("checksCount", otpInstance.getChecksCount());
    struct.put("errorCount", otpInstance.getErrorCount());
    struct.put("creationDate", otpInstance.getCreationDate());
    struct.put("latestUpdate", otpInstance.getLatestUpdate());
    struct.put("latestSuccess", otpInstance.getLatestSuccess());
    struct.put("latestError", otpInstance.getLatestError());
    struct.put("expirationDate", otpInstance.getExpirationDate());
    return struct;
  }
  

  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OTPInstance unpack(SchemaAndValue schemaAndValue)
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

    // transf en terneaire  null : null
    // (schema.field("fullStatistics") != null) ? valueStruct.getBoolean("fullStatistics") : false;
    
    String otpTypeID = valueStruct.getString("otpType");
    OTPStatus otpStatus = OTPStatus.fromExternalRepresentation(valueStruct.getString("otpStatus"));
    String otpValue = valueStruct.getString("otpValue");
    Integer	checksCount = valueStruct.getInt32("checksCount");
    Integer	errorCount = valueStruct.getInt32("errorCount");
    Date creationDate = (Date) valueStruct.get("creationDate");
    Date latestUpdate = (Date) valueStruct.get("latestUpdate");
    Date latestSuccess = (Date) valueStruct.get("latestSuccess");
    Date latestError = (Date) valueStruct.get("latestError");
    Date expirationDate = (Date) valueStruct.get("expirationDate");

    return new OTPInstance( otpTypeID,  otpStatus, otpValue, checksCount, errorCount,
  		   creationDate, latestUpdate, latestSuccess, latestError, expirationDate);
  }


  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
		    return
		        "otp [" + (otpTypeDisplayName != null ? "otpType=" + otpTypeDisplayName + ", " : "")
		        + (otpStatus != null ? "otpStatus=" + otpStatus + ", " : "")
		        + (otpValue != null ? "otpValue=" + otpValue + ", " : "")
		        + (checksCount != null ? "checksCount=" + checksCount + ", " : "")
		        + (errorCount != null ? "errorCount=" + errorCount + ", " : "")
		        + (creationDate != null ? "creationDate=" + creationDate + ", " : "")
		        + (latestSuccess != null ? "latestSuccess=" + latestSuccess + ", " : "")
		        + (latestError != null ? "latestError=" + latestError + ", " : "")
		        + (latestUpdate != null ? "latestUpdate=" + latestUpdate + ", " : "")
		        + "expirationDate=" + expirationDate + "]";

  }
    
  // Constructor used for for copy
  private OTPInstance(OTPInstance inst)
  {
		 this.otpTypeDisplayName = inst.otpTypeDisplayName;
		 this.otpStatus = inst.otpStatus;
		 this.otpValue = inst.otpValue;
		 this.checksCount = inst.checksCount;
		 this.errorCount = inst.errorCount;
		 this.creationDate = inst.creationDate;
		 this.latestUpdate = inst.latestUpdate;
		 this.latestSuccess = inst.latestSuccess;
		 this.latestError = inst.latestError;
		 this.expirationDate = inst.expirationDate;
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public OTPInstance copy()
  {
    return new OTPInstance(this);
  }
  
  
  private Object getTimeOrNull(Date date)
  {
    return (date == null) ? null : date.getTime();
  }

  // usefull for Subscriber serde
public JSONObject getJSON() {
	    JSONObject result = new JSONObject();
	    result.put("otpType", getOTPTypeDisplayName());
	    result.put("otpStatus", getOTPStatus());
	    result.put("otpValue", getOTPValue());
	    result.put("checksCount", getChecksCount());
	    result.put("errorCount", getErrorCount());
	    result.put("creationDate", getTimeOrNull(getCreationDate()));
	    result.put("latestUpdate", getTimeOrNull(getLatestUpdate()));
	    result.put("latestSuccess", getTimeOrNull(getLatestSuccess()));
	    result.put("latestError", getTimeOrNull(getLatestError()));
	    result.put("expirationDate", getTimeOrNull(getExpirationDate()));
	    return result;
}

  
}
