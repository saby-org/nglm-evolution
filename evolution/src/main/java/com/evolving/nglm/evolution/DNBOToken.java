/****************************************************************************
*
*  DNBOToken.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.Token;
import com.evolving.nglm.evolution.Token.TokenStatus;

import com.rii.utilities.SystemTime;

public class DNBOToken extends Token {

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
    schemaBuilder.name("dnbo_token");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("isAutoBounded", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("isAutoRedeemed", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("presentedOfferIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("acceptedOfferID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<DNBOToken> serde = new ConnectSerde<DNBOToken>(schema, false, DNBOToken.class, DNBOToken::pack, DNBOToken::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOToken> serde() { return serde; }

  /****************************************
   *
   * data
   *
   ****************************************/

  private String presentationStrategyID;        // reference to the {@link PresentationStrategy} used to configure the Token.
  private boolean isAutoBounded;
  private boolean isAutoRedeemed;
  private List<String> presentedOfferIDs;      // list of offersIDs presented to the subscriber, in the same order they were presented.
  private String acceptedOfferID;               // offer that has been accepted  if any, null otherwise.

  /****************************************
   *
   * accessors - basic
   *
   ****************************************/

  //
  // accessor
  //

  public String getPresentationStrategyID() { return presentationStrategyID; }
  public boolean isAutoBounded() { return isAutoBounded; }
  public boolean isAutoRedeemed() { return isAutoRedeemed; }
  public List<String> getPresentedOfferIDs() { return presentedOfferIDs; }
  public String getAcceptedOfferID() { return acceptedOfferID; }

  //
  //  setters
  //

  public void setPresentationStrategyID(String presentationStrategyID) { this.presentationStrategyID = presentationStrategyID; }
  public void setAutoBounded(boolean isAutoBounded) { this.isAutoBounded = isAutoBounded; }
  public void setAutoRedeemed(boolean isAutoRedeemed) { this.isAutoRedeemed = isAutoRedeemed; }
  public void setPresentedOfferIDs(List<String> presentedOfferIDs) { this.presentedOfferIDs = presentedOfferIDs; }
  public void setAcceptedOfferID(String acceptedOfferID) { this.acceptedOfferID = acceptedOfferID; }

  /*****************************************
   *
   * constructor (simple)
   *
   *****************************************/

  public DNBOToken(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundedDate,
      Date redeemedDate, Date tokenExpirationDate, int boundedCount, String eventID,
      String subscriberID, String tokenTypeID, String moduleID, Integer featureID,
      String presentationStrategyID, boolean isAutoBounded, boolean isAutoRedeemed,
      List<String> presentedOfferIDs, String acceptedOfferID) {
    super(tokenCode, tokenStatus, creationDate, boundedDate, redeemedDate, tokenExpirationDate,
        boundedCount, eventID, subscriberID, tokenTypeID, moduleID, featureID);
    this.presentationStrategyID = presentationStrategyID;
    this.isAutoBounded = isAutoBounded;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.acceptedOfferID = acceptedOfferID;
  }

  /*****************************************
   *
   * constructor (empty token created from outside)
   *
   *****************************************/

  public DNBOToken(String tokenCode, String subscriberID, TokenType tokenType) {
    this(tokenCode,
        TokenStatus.New,                                                 // status
        null,                                                            // creationDate
        null,                                                            // boundedDate
        null,                                                            // redeemedDate
        tokenType.getExpirationDate(SystemTime.getCurrentTime()),        // tokenExpirationDate
        0,                                                               // boundedCount
        null,                                                            // eventID
        subscriberID,                                                    // subscriberID
        tokenType.getTokenTypeID(),                                      // tokenTypeID
        DeliveryRequest.Module.REST_API.getExternalRepresentation(),     // moduleID
        null,                                                            // featureID
        null,                                                            // presentationStrategyID
        false,                                                           // isAutoBounded
        false,                                                           // isAutoRedeemed
        new ArrayList<String>(),                                         // presentedOfferIDs
        null);                                                           // acceptedOfferID
  }

  /*****************************************
   *
   * constructor (unpack)
   *
   *****************************************/

  protected DNBOToken(SchemaAndValue schemaAndValue, String presentationStrategyID, boolean isAutoBounded, boolean isAutoRedeemed,
      List<String> presentedOfferIDs, String acceptedOfferID)
  {
    super(schemaAndValue);
    this.presentationStrategyID = presentationStrategyID;
    this.isAutoBounded = isAutoBounded;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.acceptedOfferID = acceptedOfferID;

  }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    DNBOToken dnboToken = (DNBOToken) value;
    Struct struct = new Struct(schema);
    packCommon(struct, dnboToken);

    struct.put("presentationStrategyID",dnboToken.getPresentationStrategyID());
    struct.put("isAutoBounded", dnboToken.isAutoBounded());
    struct.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
    struct.put("presentedOfferIDs", dnboToken.getPresentedOfferIDs());
    struct.put("acceptedOfferID", dnboToken.getAcceptedOfferID());
    return struct;
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static DNBOToken unpack(SchemaAndValue schemaAndValue)
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
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    boolean isAutoBounded = valueStruct.getBoolean("isAutoBounded");
    boolean isAutoRedeemed = valueStruct.getBoolean("isAutoRedeemed");
    List<String> presentedOfferIDs = (List<String>) valueStruct.get("presentedOfferIDs");
    String acceptedOfferID = valueStruct.getString("acceptedOfferID");

    //
    // validate
    //

    //
    // return
    //

    return new DNBOToken(schemaAndValue,
        presentationStrategyID, isAutoBounded, isAutoRedeemed,
        presentedOfferIDs, acceptedOfferID);
  }
}
