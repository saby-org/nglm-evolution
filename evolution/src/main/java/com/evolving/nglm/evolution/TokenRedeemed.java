/*****************************************************************************
*
*  TokenRedeemed.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import com.evolving.nglm.core.SubscriberStreamOutput;
import org.apache.kafka.connect.data.Field;
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

public class TokenRedeemed extends SubscriberStreamOutput implements EvolutionEngineEvent
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
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

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }


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

  public TokenRedeemed(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, String tokenType, String acceptedOfferId)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.tokenType = tokenType;
    this.acceptedOfferId = acceptedOfferId;
  }

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
    TokenRedeemed tokenRedeemed = (TokenRedeemed) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,tokenRedeemed);
    struct.put("subscriberID", tokenRedeemed.getSubscriberID());
    struct.put("eventDate", tokenRedeemed.getEventDate());
    struct.put("tokenType", tokenRedeemed.getTokenType());
    struct.put("acceptedOfferId", tokenRedeemed.getAcceptedOfferId());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

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

    return new TokenRedeemed(schemaAndValue, subscriberID, eventDate, tokenType, acceptedOfferId);
  }
}
