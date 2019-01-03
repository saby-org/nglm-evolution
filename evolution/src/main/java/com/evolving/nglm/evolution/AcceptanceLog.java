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
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("msisdn", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("presentationStrategyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("presentationToken", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);    
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

  private String subscriberID;
  private String msisdn;
  private String offerID;
  private String presentationStrategyID;
  private String presentationToken;
  private String channelID;
  private String salesChannelID;  
  private String userID;
  private Date eventDate;
  private Date fulfilledDate;
  private Integer position;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public String getMsisdn() { return msisdn; }  
  public String getOfferID() { return offerID; }
  public String getPresentationStrategyID() { return presentationStrategyID; }
  public String getPresentationToken() { return presentationToken; }
  public String getChannelID() { return channelID; }
  public String getSalesChannelID() { return salesChannelID; }  
  public String getUserID() { return userID; }
  public Date getEventDate() { return eventDate; }
  public Date getFulfilledDate() { return fulfilledDate; }
  public Integer getPosition() { return position; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AcceptanceLog(String subscriberID, String msisdn, String offerID, String presentationStrategyID, String presentationToken, String channelID, String salesChannelID, String userID, Date eventDate, Date fulfilledDate, Integer position)
  {
    this.subscriberID = subscriberID;
    this.msisdn = msisdn;    
    this.offerID = offerID;
    this.presentationStrategyID = presentationStrategyID;
    this.presentationToken = presentationToken;
    this.channelID = channelID;
    this.salesChannelID = salesChannelID;    
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
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.msisdn = JSONUtilities.decodeString(jsonRoot, "msisdn", true);
    this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
    this.presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "presentationStrategyID", true);
    this.presentationToken = JSONUtilities.decodeString(jsonRoot, "presentationToken", false);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);    
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
    struct.put("subscriberID", acceptanceLog.getSubscriberID());
    struct.put("msisdn", acceptanceLog.getMsisdn());
    struct.put("offerID", acceptanceLog.getOfferID());
    struct.put("presentationStrategyID", acceptanceLog.getPresentationStrategyID());
    struct.put("presentationToken", acceptanceLog.getPresentationToken());
    struct.put("channelID", acceptanceLog.getChannelID());
    struct.put("salesChannelID", acceptanceLog.getSalesChannelID());    
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
    String subscriberID = valueStruct.getString("subscriberID");
    String msisdn = valueStruct.getString("msisdn");    
    String offerID = valueStruct.getString("offerID");
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    String presentationToken = valueStruct.getString("presentationToken");
    String channelID = valueStruct.getString("channelID");
    String salesChannelID = valueStruct.getString("salesChannelID");
    String userID = valueStruct.getString("userID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    Date fulfilledDate = (Date) valueStruct.get("fulfilledDate");
    Integer position = valueStruct.getInt32("position");
    
    //
    //  return
    //

    return new AcceptanceLog(subscriberID, msisdn, offerID, presentationStrategyID, presentationToken, channelID, salesChannelID, userID, eventDate, fulfilledDate, position);
  }
}
