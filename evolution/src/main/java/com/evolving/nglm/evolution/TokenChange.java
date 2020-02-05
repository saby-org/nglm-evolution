/****************************************************************************
*
*  TokenChange.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;


public class TokenChange implements EvolutionEngineEvent, SubscriberStreamOutput, Action
{
  
  // static
  
  public static String CREATE   = "Create";
  public static String ALLOCATE = "Allocate";
  public static String REDEEM   = "Redeem";
  public static String RESEND   = "Resend";
  public static String REFUSE   = "Refuse";
  public static String EXTEND   = "Extend";
  
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
    schemaBuilder.name("token_change");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDateTime", Timestamp.builder().schema());
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("action", Schema.STRING_SCHEMA);
    schemaBuilder.field("returnStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<TokenChange> serde = new ConnectSerde<TokenChange>(schema, false, TokenChange.class, TokenChange::pack, TokenChange::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<TokenChange> serde() { return serde; }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDateTime;
  private String eventID;
  private String tokenCode;
  private String action;
  private String returnStatus;
  private String origin;
  private String moduleID;
  private Integer featureID;
  
  /****************************************
  *
  * accessors - basic
  *
  ****************************************/

  //
  // accessor
  //

  @Override
  public String getSubscriberID() { return subscriberID; }
  public Date geteventDateTime() { return eventDateTime; }
  public String getEventID() { return eventID; }
  public String getTokenCode() { return tokenCode; }
  public String getAction() { return action; }
  public String getReturnStatus() { return returnStatus; }
  public String getOrigin() { return origin; }
  public String getModuleID() { return moduleID; }
  public Integer getFeatureID() { return featureID; }
  public ActionType getActionType() { return ActionType.TokenChange; }

  //
  //  setters
  //

  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void seteventDateTime(Date eventDateTime) { this.eventDateTime = eventDateTime; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setTokenCode(String tokenCode) { this.tokenCode = tokenCode; }
  public void setAction(String action) { this.action = action; }
  public void setReturnStatus(String returnStatus) { this.returnStatus = returnStatus; }
  public void setOrigin(String origin) { this.origin = origin; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setFeatureID(Integer featureID) { this.featureID = featureID; }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public TokenChange(String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, Module module, Integer featureID)
  {
    this(subscriberID, eventDateTime, eventID, tokenCode, action, returnStatus, origin, module.getExternalRepresentation(), featureID);
  }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public TokenChange(String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, String moduleID, Integer featureID)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.tokenCode = tokenCode;
    this.action = action;
    this.returnStatus = returnStatus;
    this.origin = origin;
    this.moduleID = moduleID;
    this.featureID = featureID;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    TokenChange tokenChange = (TokenChange) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID",tokenChange.getSubscriberID());
    struct.put("eventDateTime",tokenChange.geteventDateTime());
    struct.put("eventID", tokenChange.getEventID());
    struct.put("tokenCode", tokenChange.getTokenCode());
    struct.put("action", tokenChange.getAction());
    struct.put("returnStatus", tokenChange.getReturnStatus());
    struct.put("origin", tokenChange.getOrigin());
    struct.put("moduleID", tokenChange.getModuleID());
    struct.put("featureID", tokenChange.getFeatureID());
    return struct;
  }

  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static TokenChange unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDateTime = (Date) valueStruct.get("eventDateTime");
    String eventID = valueStruct.getString("eventID");
    String tokenCode = valueStruct.getString("tokenCode");
    String action = valueStruct.getString("action");
    String returnStatus = valueStruct.getString("returnStatus");
    String origin = valueStruct.getString("origin");
    String moduleID = (schemaVersion >=2 ) ? valueStruct.getString("moduleID") : "";
    Integer featureID = (schemaVersion >=2 ) ? valueStruct.getInt32("featureID") : 0;

    //
    // validate
    //

    //
    // return
    //

    return new TokenChange(subscriberID, eventDateTime, eventID, tokenCode, action, returnStatus, origin, moduleID, featureID);
  }
  
  
  @Override
  public Date getEventDate()
  {
    return geteventDateTime();
  }
  
  @Override
  public Schema subscriberStreamEventSchema()
  {
    return schema;
  }
  
  @Override
  public Object subscriberStreamEventPack(Object value)
  {
    return pack(value);
  }
  
  @Override
  public String getEventName()
  {
    return "token change";
  }
}
