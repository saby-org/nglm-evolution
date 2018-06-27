/*****************************************************************************
*
*  InteractionLog.java
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

public class InteractionLog implements SubscriberStreamEvent
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
    schemaBuilder.name("interaction_log");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("isAvailable", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<InteractionLog> serde = new ConnectSerde<InteractionLog>(schema, false, InteractionLog.class, InteractionLog::pack, InteractionLog::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<InteractionLog> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private String channelID;
  private String userID;
  private Date eventDate;
  private Boolean isAvailable;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public String getChannelID() { return channelID; }
  public String getUserID() { return userID; }
  public Date getEventDate() { return eventDate; }
  public Boolean getIsAvailable() { return isAvailable; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public InteractionLog(String subscriberID, String channelID, String userID, Date eventDate, Boolean isAvailable)
  {
    this.subscriberID = subscriberID;
    this.channelID = channelID;
    this.userID = userID;
    this.eventDate = eventDate;
    this.isAvailable = isAvailable;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public InteractionLog(JSONObject jsonRoot)
  {
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.eventDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
    this.isAvailable = JSONUtilities.decodeBoolean(jsonRoot, "isAvailable", false);
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
    InteractionLog interactionLog = (InteractionLog) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", interactionLog.getSubscriberID());
    struct.put("channelID", interactionLog.getChannelID());
    struct.put("userID", interactionLog.getUserID());
    struct.put("eventDate", interactionLog.getEventDate());
    struct.put("isAvailable", interactionLog.getIsAvailable());
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

  public static InteractionLog unpack(SchemaAndValue schemaAndValue)
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
    String subscriberID = valueStruct.getString("subscriberID");
    String channelID = valueStruct.getString("channelID");
    String userID = valueStruct.getString("userID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    Boolean isAvailable = valueStruct.getBoolean("isAvailable");
    
    //
    //  return
    //

    return new InteractionLog(subscriberID, channelID, userID, eventDate, isAvailable);
  }
}