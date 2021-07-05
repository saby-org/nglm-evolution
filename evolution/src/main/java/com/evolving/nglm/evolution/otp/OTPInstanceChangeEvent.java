/*****************************************************************************
*
*  OTPInstanceChangeEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.otp;

import java.util.Date;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
//import com.evolving.nglm.evolution.ActionManager.Action;


public class OTPInstanceChangeEvent extends SubscriberStreamOutput implements EvolutionEngineEvent //, Action
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
	    schemaBuilder.name("otpInstanceChangeEvent");
	    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),9));
	    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
	    // IN + OUT
	    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
	    schemaBuilder.field("eventDate", Timestamp.builder().schema());
	    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
	    schemaBuilder.field("action", Schema.STRING_SCHEMA);
	    schemaBuilder.field("otpTypeName", Schema.STRING_SCHEMA);
	    // IN
	    schemaBuilder.field("otpCheckValue", Schema.OPTIONAL_STRING_SCHEMA);
	    // OUT   
	    schemaBuilder.field("returnStatus", Schema.OPTIONAL_STRING_SCHEMA);
	    schemaBuilder.field("remainingAttempts", Schema.OPTIONAL_STRING_SCHEMA);
	    schemaBuilder.field("currentTypeErrors", Schema.OPTIONAL_STRING_SCHEMA);
	    schemaBuilder.field("globalErrorCounts", Schema.OPTIONAL_STRING_SCHEMA);
	    // not needed yet // schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
	    schema = schemaBuilder.build();
	  };

	    
	  //
	  // serde
	  //

	  private static ConnectSerde<OTPInstanceChangeEvent> serde = new ConnectSerde<OTPInstanceChangeEvent>(schema, false, OTPInstanceChangeEvent.class, OTPInstanceChangeEvent::pack, OTPInstanceChangeEvent::unpack);

	  //
	  // constants
	  //
	  
	  
	  
	  //action
	  public enum OTPChangeAction {
		  Check("Check"),
		  Generate("Generate"),
		  Burn("Burn"),
		  Ban("Ban"),
		  Allow("Allow"),
		  Unknown("(Unknown)");
		  
	    private String externalRepresentation;
	    private OTPChangeAction(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
	    public String getExternalRepresentation() { return externalRepresentation; }
	    public static OTPChangeAction fromExternalRepresentation(String externalRepresentation) { for (OTPChangeAction enumeratedValue : OTPChangeAction.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
	  }
	  

	  //
	  // accessor
	  //

	  public static Schema schema() { return schema; }
	  public static ConnectSerde<OTPInstanceChangeEvent> serde() { return serde; }
	  public Schema subscriberStreamEventSchema() { return schema(); }

	  /****************************************
	  *
	  * data
	  *
	  ****************************************/

	  private Date eventDate;
	  private String subscriberID;
	  private String eventID;
	  private OTPChangeAction action;
	  private String otpTypeName;
	  private String otpCheckValue;
	  private String remainingAttempts;
	  private String currentTypeErrors;
	  private String globalErrorCounts;
	  private RESTAPIGenericReturnCodes returnStatus;
//	  private int tenantID;

	  
	  
	  /****************************************
	  *
	  * accessors
	  *
	  ****************************************/

	  public String getEventName() { return "otpInstanceChangeEvent"; }
	  @Override
	  public String getSubscriberID() { return subscriberID; }
	  @Override
	  public Date getEventDate() { return eventDate; }
	  public String getEventID() { return eventID; }
	  public OTPChangeAction getAction() { return action; }
	  public String getOTPTypeName() { return otpTypeName; }
	  public String getOTPCheckValue() { return otpCheckValue; }
	  public String getRemainingAttempts() { return remainingAttempts; }
	  public String getCurrentTypeErrors() { return currentTypeErrors; }
	  public String getGlobalErrorCounts() { return globalErrorCounts; }
	  public RESTAPIGenericReturnCodes getReturnStatus() { return returnStatus; }
	  
	  /*****************************************
	  *
	  * constructor
	  *
	  *****************************************/

	  public OTPInstanceChangeEvent(Date eventDate, String subscriberID, String eventID, OTPChangeAction action,
			String otpTypeName, String otpCheckValue, String remainingAttempts, String currentTypeErrors,
			String globalErrorCounts, RESTAPIGenericReturnCodes returnStatus//, int tenantID
			) {
		//super();
		this.eventDate = eventDate;
		this.subscriberID = subscriberID;
		this.eventID = eventID;
		this.action = action;
		this.otpTypeName = otpTypeName;
		this.otpCheckValue = otpCheckValue;
		this.remainingAttempts = remainingAttempts;
		this.currentTypeErrors = currentTypeErrors;
		this.globalErrorCounts = globalErrorCounts;
		this.returnStatus = returnStatus;
		//this.tenantID = tenantID;
	}


	  public OTPInstanceChangeEvent(OTPInstanceChangeEvent eventToClone ) {
		//super();
		this.eventDate = eventToClone.getEventDate();
		this.subscriberID = eventToClone.getSubscriberID();
		this.eventID = eventToClone.getEventID();
		this.action = eventToClone.getAction();
		this.otpTypeName = eventToClone.getOTPTypeName();
		this.otpCheckValue = eventToClone.getOTPCheckValue();
		this.remainingAttempts = eventToClone.getRemainingAttempts();
		this.currentTypeErrors = eventToClone.getCurrentTypeErrors();
		this.globalErrorCounts = eventToClone.getGlobalErrorCounts();
		this.returnStatus = eventToClone.getReturnStatus();
		//this.tenantID = eventToClone.getTenantID();
	}

	  /*****************************************
	  *
	  * constructor unpack
	  *
	  *****************************************/

	  public OTPInstanceChangeEvent(SchemaAndValue schemaAndValue, 
			  Date eventDate, String subscriberID, String eventID, OTPChangeAction action,
				String otpTypeName, String otpCheckValue, String remainingAttempts, String currentTypeErrors,
				String globalErrorCounts, RESTAPIGenericReturnCodes returnStatus//, int tenantID
				) {
	    super(schemaAndValue);
		this.eventDate = eventDate;
		this.subscriberID = subscriberID;
		this.eventID = eventID;
		this.action = action;
		this.otpTypeName = otpTypeName;
		this.otpCheckValue = otpCheckValue;
		this.remainingAttempts = remainingAttempts;
		this.currentTypeErrors = currentTypeErrors;
		this.globalErrorCounts = globalErrorCounts;
		this.returnStatus = returnStatus;
		//this.tenantID = tenantID;
	  }

	  @Override
	  public String toString() {
	    return "OTPInstanceChange{" +
	            "subscriberID='" + subscriberID + '\'' +
	            ", eventDate=" + eventDate +
	            ", eventID='" + eventID + '\'' +
	            ", action=" + action.getExternalRepresentation() +
	            ", otpTypeName='" + otpTypeName + '\'' +
	            ", otpCheckValue='" + otpCheckValue + '\'' +
	            ", remainingAttempts='" + remainingAttempts + '\'' +
	            ", currentTypeErrors='" + currentTypeErrors + '\'' +
	            ", globalErrorCounts='" + globalErrorCounts + '\'' +
	            ", returnStatus=" + returnStatus.getGenericResponseMessage() +
	            //'\'' +", tenantID='" + tenantID + 
	            '}';
	  }

	  
	  /*****************************************
	  *
	  * pack
	  *
	  *****************************************/

	  public static Object pack(Object value)
	  {
		OTPInstanceChangeEvent otpInstanceChangeEvent = (OTPInstanceChangeEvent) value;
	    Struct struct = new Struct(schema);
	    packSubscriberStreamOutput(struct,otpInstanceChangeEvent);
	    
	    struct.put("eventDate", otpInstanceChangeEvent.getEventDate());
	    struct.put("subscriberID", otpInstanceChangeEvent.getSubscriberID());
	    struct.put("eventID", otpInstanceChangeEvent.getEventID());
		struct.put("action", otpInstanceChangeEvent.getAction().getExternalRepresentation());
		struct.put("otpTypeName", otpInstanceChangeEvent.getOTPTypeName());
		struct.put("otpCheckValue", otpInstanceChangeEvent.getOTPCheckValue());
		struct.put("remainingAttempts", otpInstanceChangeEvent.getRemainingAttempts());
		struct.put("currentTypeErrors", otpInstanceChangeEvent.getCurrentTypeErrors());
		struct.put("globalErrorCounts", otpInstanceChangeEvent.getGlobalErrorCounts());
		struct.put("returnStatus", otpInstanceChangeEvent.getReturnStatus().getGenericResponseMessage());
		//struct.put("tenantID", (short) otpInstanceChangeEvent.getTenantID());
	    return struct;
	  }

	  //
	  // subscriberStreamEventPack
	  //

	  public Object subscriberStreamEventPack(Object value) { return pack(value); }

	  /*****************************************
	  *
	  * unpack
	  *
	  *****************************************/

	  public static OTPInstanceChangeEvent unpack(SchemaAndValue schemaAndValue)
	  {
	    //
	    // data
	    //

	    Schema schema = schemaAndValue.schema();
	    Object value = schemaAndValue.value();
	    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

	    //
	    // unpack
	    //

	    Struct valueStruct = (Struct) value;
	    String subscriberID = valueStruct.getString("subscriberID");
	    Date eventDate = (Date) valueStruct.get("eventDate");
	      String eventID = valueStruct.getString("eventID");
	      OTPChangeAction action = OTPChangeAction.fromExternalRepresentation(valueStruct.getString("action"));
	      String otpTypeName = valueStruct.getString("otpTypeName");
	      String otpCheckValue = valueStruct.getString("otpCheckValue");
	      String remainingAttempts = valueStruct.getString("remainingAttempts");
	      String currentTypeErrors = valueStruct.getString("currentTypeErrors");
	      String globalErrorCounts = valueStruct.getString("globalErrorCounts");
	      RESTAPIGenericReturnCodes returnStatus = RESTAPIGenericReturnCodes.fromGenericResponseMessage(valueStruct.getString("returnStatus"));
//	      int tenantID = (schemaVersion >= 9)? valueStruct.getInt16("tenantID") : 1; // for old system, default to tenant 1

	      //
		  // return
		  //
	   
	      return new OTPInstanceChangeEvent( schemaAndValue, 
				   eventDate,  subscriberID,  eventID,  action,
					 otpTypeName,  otpCheckValue,  remainingAttempts,  currentTypeErrors,
					 globalErrorCounts,  returnStatus//, int tenantID
					);
	  }



		  public void setReturnStatus(RESTAPIGenericReturnCodes returnStatus) { this.returnStatus = returnStatus; }
		  
		  
}
