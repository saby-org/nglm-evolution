/*****************************************************************************
*
*  TokenRedeemed.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class TokenRedeemed implements EvolutionEngineEvent
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
    schemaBuilder.name("token_redeemed");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("tokenType", Schema.STRING_SCHEMA);
    schemaBuilder.field("acceptedOfferId", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<TokenRedeemed> serde = new ConnectSerde<TokenRedeemed>(schema, false, TokenRedeemed.class, TokenRedeemed::pack, TokenRedeemed::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<TokenRedeemed> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private String tokenType;
  private String acceptedOfferId;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getEventName() { return "tokenRedeemed"; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getTokenType() { return tokenType; }
  public String getAcceptedOfferId() { return acceptedOfferId; }


  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public TokenRedeemed(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  simple attributes
    *
    *****************************************/

    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    String date = JSONUtilities.decodeString(jsonRoot, "eventDate", false);
    if(date != null)
      {
        this.eventDate = GUIManagedObject.parseDateField(date);
      }
    else
      {
        this.eventDate = SystemTime.getCurrentTime();
      }
    this.tokenType = JSONUtilities.decodeString(jsonRoot, "tokenType", true);
    this.acceptedOfferId = JSONUtilities.decodeString(jsonRoot, "acceptedOfferId", true);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public TokenRedeemed(String subscriberID, Date eventDate, String tokenType, String acceptedOfferId)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.tokenType = tokenType;
    this.acceptedOfferId = acceptedOfferId;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    TokenRedeemed lifeCycleSegment = (TokenRedeemed) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", lifeCycleSegment.getSubscriberID());
    struct.put("eventDate", lifeCycleSegment.getEventDate());
    struct.put("tokenType", lifeCycleSegment.getTokenType());
    struct.put("acceptedOfferId", lifeCycleSegment.getAcceptedOfferId());
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

  public static TokenRedeemed unpack(SchemaAndValue schemaAndValue)
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
    Date eventDate = (Date) valueStruct.get("eventDate");
    String tokenType = valueStruct.getString("tokenType");
    String acceptedOfferId = valueStruct.getString("acceptedOfferId");

    //
    //  return
    //

    return new TokenRedeemed(subscriberID, eventDate, tokenType, acceptedOfferId);
  }
}
