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
  *  standard formats
  *
  *****************************************/

  private static SimpleDateFormat standardDateFormat;
  static
  {
    standardDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXXX");
    standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
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
    schemaBuilder.name("acceptance_log");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("transactionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("fulfilledDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("position", Schema.OPTIONAL_INT32_SCHEMA);
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

  private String transactionID;
  private String subscriberID;
  private String offerID;
  private String channelID;
  private String userID;
  private Date eventDate;
  private Date fulfilledDate;
  private Integer position;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getTransactionID() { return transactionID; }
  public String getSubscriberID() { return subscriberID; }
  public String getOfferID() { return offerID; }
  public String getChannelID() { return channelID; }
  public String getUserID() { return userID; }
  public Date getEventDate() { return eventDate; }
  public Date getFulfilledDate() { return fulfilledDate; }
  public Integer getPosition() { return position; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AcceptanceLog(String transactionID, String subscriberID, String offerID, String channelID, String userID, Date eventDate, Date fulfilledDate, Integer position)
  {
    this.transactionID = transactionID;
    this.subscriberID = subscriberID;
    this.offerID = offerID;
    this.channelID = channelID;
    this.userID = userID;
    this.eventDate = eventDate;
    this.fulfilledDate = fulfilledDate;
    this.position = position;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public AcceptanceLog(JSONObject jsonRoot)
  {
    this.transactionID = JSONUtilities.decodeString(jsonRoot, "transactionID", true);
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.eventDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
    this.fulfilledDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "fulfilledDate", false));
    this.position = JSONUtilities.decodeInteger(jsonRoot, "position", false);
  }

  /*****************************************
  *
  *  parseDateField
  *
  *****************************************/

  private Date parseDateField(String stringDate) throws JSONUtilitiesException
  {
    Date result = null;
    try
      {
        if (stringDate != null && stringDate.trim().length() > 0)
          {
            synchronized (standardDateFormat)
              {
                result = standardDateFormat.parse(stringDate.trim());
              }
          }
      }
    catch (ParseException e)
      {
        throw new JSONUtilitiesException("parseDateField", e);
      }
    return result;
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
    struct.put("transactionID", acceptanceLog.getTransactionID());
    struct.put("subscriberID", acceptanceLog.getSubscriberID());
    struct.put("offerID", acceptanceLog.getOfferID());
    struct.put("channelID", acceptanceLog.getChannelID());
    struct.put("userID", acceptanceLog.getUserID());
    struct.put("eventDate", acceptanceLog.getEventDate());
    struct.put("fulfilledDate", acceptanceLog.getFulfilledDate());
    struct.put("position", acceptanceLog.getPosition());
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
    String transactionID = valueStruct.getString("transactionID");
    String subscriberID = valueStruct.getString("subscriberID");
    String offerID = valueStruct.getString("offerID");
    String channelID = valueStruct.getString("channelID");
    String userID = valueStruct.getString("userID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    Date fulfilledDate = (Date) valueStruct.get("fulfilledDate");
    Integer position = valueStruct.getInt32("position");
    
    //
    //  return
    //

    return new AcceptanceLog(transactionID, subscriberID, offerID, channelID, userID, eventDate, fulfilledDate, position);
  }
}