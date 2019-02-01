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
import org.json.simple.JSONArray;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
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
    schemaBuilder.field("offerIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("positions", SchemaBuilder.array(Schema.INT32_SCHEMA));
    schemaBuilder.field("presentationStrategyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);    
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Schema.INT64_SCHEMA);
    schemaBuilder.field("presentationToken", Schema.OPTIONAL_STRING_SCHEMA);
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
  private List<String> offerIDs;
  private List<Integer> positions;
  private String presentationStrategyID;
  private String channelID;
  private String salesChannelID;  
  private String userID;
  private Date eventDate;
  private String presentationToken;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public List<String> getOfferIDs() { return offerIDs; }
  public List<Integer> getPositions() { return positions; }
  public String getPresentationStrategyID() { return presentationStrategyID; }
  public String getChannelID() { return channelID; }
  public String getSalesChannelID() { return salesChannelID; }  
  public String getUserID() { return userID; }
  public Date getEventDate() { return eventDate; }
  public String getPresentationToken() { return presentationToken; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationLog(String subscriberID, List<String> offerIDs, List<Integer> positions, String presentationStrategyID, String channelID, String salesChannelID, String userID, Date eventDate, String presentationToken)
  {
    this.subscriberID = subscriberID;
    this.offerIDs = offerIDs;
    this.positions = positions;
    this.presentationStrategyID = presentationStrategyID;
    this.channelID = channelID;
    this.salesChannelID = salesChannelID;    
    this.userID = userID;
    this.eventDate = eventDate;
    this.presentationToken = presentationToken;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PresentationLog(JSONObject jsonRoot)
  {
    //
    //  attributes
    //
    
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.offerIDs = decodeOfferIDs(JSONUtilities.decodeJSONArray(jsonRoot, "offerIDs", false));
    this.offerIDs = (this.offerIDs == null) ? Collections.<String>singletonList(JSONUtilities.decodeString(jsonRoot, "offerID", true)) : this.offerIDs;
    this.positions = decodePositions(JSONUtilities.decodeJSONArray(jsonRoot, "positions", false));
    this.positions = (this.positions == null) ? Collections.<Integer>singletonList(JSONUtilities.decodeInteger(jsonRoot, "position", false)) : this.positions;
    this.presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "presentationStrategyID", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);    
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.eventDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
    this.presentationToken = JSONUtilities.decodeString(jsonRoot, "presentationToken", false);

    //
    //  
    //
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
  *  decodeOfferIDs
  *
  *****************************************/

  private List<String> decodeOfferIDs(JSONArray jsonArray)
  {
    List<String> offerIDs = null;
    if (jsonArray != null)
      {
        offerIDs = new ArrayList<String>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            String offerID = (String) jsonArray.get(i);
            offerIDs.add(offerID);
          }
      }
    return offerIDs;
  }

  /*****************************************
  *
  *  decodePositions
  *
  *****************************************/

  private List<Integer> decodePositions(JSONArray jsonArray)
  {
    List<Integer> positions = null;
    if (jsonArray != null)
      {
        positions = new ArrayList<Integer>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            Long positionLong = (Long) jsonArray.get(i);
            Integer position = (positionLong != null) ? new Integer(positionLong.intValue()) : null;
            positions.add(position);
          }
      }
    return positions;
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
    struct.put("offerIDs", presentationLog.getOfferIDs());
    struct.put("positions", presentationLog.getPositions());
    struct.put("presentationStrategyID", presentationLog.getPresentationStrategyID());
    struct.put("channelID", presentationLog.getChannelID());
    struct.put("salesChannelID", presentationLog.getSalesChannelID());    
    struct.put("userID", presentationLog.getUserID());
    struct.put("eventDate", presentationLog.getEventDate().getTime());
    struct.put("presentationToken", presentationLog.getPresentationToken());
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
    List<String> offerIDs = (List<String>) valueStruct.get("offerIDs");
    List<Integer> positions = (List<Integer>) valueStruct.get("positions");
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    String channelID = valueStruct.getString("channelID");
    String salesChannelID = valueStruct.getString("salesChannelID");    
    String userID = valueStruct.getString("userID");
    Date eventDate = new Date(valueStruct.getInt64("eventDate"));
    String presentationToken = valueStruct.getString("presentationToken");
    
    //
    //  return
    //

    return new PresentationLog(subscriberID, offerIDs, positions, presentationStrategyID, channelID, salesChannelID, userID, eventDate, presentationToken);
  }
}
