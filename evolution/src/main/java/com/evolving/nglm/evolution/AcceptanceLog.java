/*****************************************************************************
*
*  AcceptanceLog.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class AcceptanceLog implements SubscriberStreamEvent
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
    schemaBuilder.name("acceptance_log");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("msisdn", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Schema.INT64_SCHEMA);
    schemaBuilder.field("callUniqueIdentifier", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentationToken", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("transactionDurationMs", Schema.INT32_SCHEMA);
    schemaBuilder.field("controlGroupState", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("fulfilledDate", Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field("position", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("actionCall", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<AcceptanceLog> serde = new ConnectSerde<AcceptanceLog>(schema, false, AcceptanceLog.class, AcceptanceLog::pack, AcceptanceLog::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<AcceptanceLog> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String msisdn;
  private String subscriberID;
  private Date eventDate;
  private String callUniqueIdentifier;
  private String channelID;
  private String salesChannelID;
  private String userID;
  private String presentationToken;
  private String presentationStrategyID;
  private Integer transactionDurationMs;
  private String controlGroupState;
  private String offerID;
  private Date fulfilledDate;
  private Integer position;
  private Integer actionCall;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getMsisdn() { return msisdn; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getCallUniqueIdentifier() { return callUniqueIdentifier; }
  public String getChannelID() { return channelID; }
  public String getSalesChannelID() { return salesChannelID; }
  public String getUserID() { return userID; }
  public String getPresentationToken() { return presentationToken; }
  public String getPresentationStrategyID() { return presentationStrategyID; }
  public Integer getTransactionDurationMs() { return transactionDurationMs; }
  public String getControlGroupState() { return controlGroupState; }  
  public String getOfferID() { return offerID; }
  public Date getFulfilledDate() { return fulfilledDate; }
  public Integer getPosition() { return position; }
  public Integer getActionCall() { return actionCall; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AcceptanceLog(String msisdn, String subscriberID, Date eventDate, String callUniqueIdentifier, String channelID, String salesChannelID, String userID, String presentationToken, String presentationStrategyID, Integer transactionDurationMs, String controlGroupState, String offerID, Date fulfilledDate, Integer position, Integer actionCall)
  {
    this.msisdn = msisdn;
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.callUniqueIdentifier = callUniqueIdentifier;
    this.channelID = channelID;
    this.salesChannelID = salesChannelID;
    this.userID = userID;
    this.presentationToken = presentationToken;
    this.presentationStrategyID = presentationStrategyID;
    this.transactionDurationMs = transactionDurationMs;
    this.controlGroupState = controlGroupState;
    this.offerID = offerID;
    this.fulfilledDate = fulfilledDate;
    this.position = position;
    this.actionCall = actionCall;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public AcceptanceLog(JSONObject jsonRoot)
  {
    this.msisdn = JSONUtilities.decodeString(jsonRoot, "msisdn", true);
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.eventDate = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
    this.callUniqueIdentifier = JSONUtilities.decodeString(jsonRoot, "callUniqueIdentifier", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.presentationToken = JSONUtilities.decodeString(jsonRoot, "presentationToken", false);
    this.presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "presentationStrategyID", false);
    this.transactionDurationMs = JSONUtilities.decodeInteger(jsonRoot, "transactionDurationMs", true);
    this.controlGroupState = JSONUtilities.decodeString(jsonRoot, "controlGroupState", true);
    this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
    this.fulfilledDate = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "fulfilledDate", false));
    this.position = JSONUtilities.decodeInteger(jsonRoot, "position", false);
    this.actionCall = JSONUtilities.decodeInteger(jsonRoot, "actionCall", false);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    AcceptanceLog acceptanceLog = (AcceptanceLog) value;
    Struct struct = new Struct(schema);
    struct.put("msisdn", acceptanceLog.getMsisdn());
    struct.put("subscriberID", acceptanceLog.getSubscriberID());
    struct.put("eventDate", acceptanceLog.getEventDate().getTime());
    struct.put("callUniqueIdentifier", acceptanceLog.getCallUniqueIdentifier());
    struct.put("channelID", acceptanceLog.getChannelID());
    struct.put("salesChannelID", acceptanceLog.getSalesChannelID());
    struct.put("userID", acceptanceLog.getUserID());
    struct.put("presentationToken", acceptanceLog.getPresentationToken());
    struct.put("presentationStrategyID", acceptanceLog.getPresentationStrategyID());    
    struct.put("transactionDurationMs", acceptanceLog.getTransactionDurationMs());
    struct.put("controlGroupState", acceptanceLog.getControlGroupState());
    struct.put("offerID", acceptanceLog.getOfferID());
    struct.put("fulfilledDate", acceptanceLog.getFulfilledDate().getTime());
    struct.put("position", acceptanceLog.getPosition());
    struct.put("actionCall", acceptanceLog.getActionCall());
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static AcceptanceLog unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String msisdn = valueStruct.getString("msisdn");
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = new Date(valueStruct.getInt64("eventDate"));
    String callUniqueIdentifier = valueStruct.getString("callUniqueIdentifier");
    String channelID = valueStruct.getString("channelID");
    String salesChannelID = valueStruct.getString("salesChannelID");
    String userID = valueStruct.getString("userID");
    String presentationToken = valueStruct.getString("presentationToken");
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    Integer transactionDurationMs = valueStruct.getInt32("transactionDurationMs");
    String controlGroupState = valueStruct.getString("controlGroupState");
    String offerID = valueStruct.getString("offerID");
    Date fulfilledDate = ((valueStruct.getInt64("fulfilledDate") != null) ? new Date(valueStruct.getInt64("fulfilledDate")) : null);
    Integer position = valueStruct.getInt32("position");
    Integer actionCall = valueStruct.getInt32("actionCall");
    
    //
    //  return
    //

    return new AcceptanceLog(msisdn, subscriberID, eventDate, callUniqueIdentifier, channelID, salesChannelID, userID, presentationToken, presentationStrategyID, transactionDurationMs, controlGroupState, offerID, fulfilledDate, position, actionCall);
  }
}
