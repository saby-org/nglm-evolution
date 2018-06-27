/*****************************************************************************
*
*  PresentationLog.java
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

public class PresentationLog implements SubscriberStreamEvent
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
    schemaBuilder.name("presentation_log");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("position", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PresentationLog> serde = new ConnectSerde<PresentationLog>(schema, false, PresentationLog.class, PresentationLog::pack, PresentationLog::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PresentationLog> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private String offerID;
  private String channelID;
  private String userID;
  private Date eventDate;
  private Integer position;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public String getOfferID() { return offerID; }
  public String getChannelID() { return channelID; }
  public String getUserID() { return userID; }
  public Date getEventDate() { return eventDate; }
  public Integer getPosition() { return position; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationLog(String subscriberID, String offerID, String channelID, String userID, Date eventDate, Integer position)
  {
    this.subscriberID = subscriberID;
    this.offerID = offerID;
    this.channelID = channelID;
    this.userID = userID;
    this.eventDate = eventDate;
    this.position = position;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PresentationLog(JSONObject jsonRoot)
  {
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.eventDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
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
    PresentationLog presentationLog = (PresentationLog) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", presentationLog.getSubscriberID());
    struct.put("offerID", presentationLog.getOfferID());
    struct.put("channelID", presentationLog.getChannelID());
    struct.put("userID", presentationLog.getUserID());
    struct.put("eventDate", presentationLog.getEventDate());
    struct.put("position", presentationLog.getPosition());
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

  public static PresentationLog unpack(SchemaAndValue schemaAndValue)
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
    String offerID = valueStruct.getString("offerID");
    String channelID = valueStruct.getString("channelID");
    String userID = valueStruct.getString("userID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    Integer position = valueStruct.getInt32("position");
    
    //
    //  return
    //

    return new PresentationLog(subscriberID, offerID, channelID, userID, eventDate, position);
  }
}