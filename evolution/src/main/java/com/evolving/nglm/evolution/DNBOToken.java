/****************************************************************************
*
*  DNBOToken.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Token;
import com.evolving.nglm.evolution.Token.TokenStatus;


public class DNBOToken extends Token
{
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),4));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("scoringStrategyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    schemaBuilder.field("isAutoBounded", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("isAutoRedeemed", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("presentedOfferIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("presentedOffersSalesChannel", Schema.OPTIONAL_STRING_SCHEMA);
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
  private List<String> scoringStrategyIDs;      // reference to the {@link ScoringStrategy}s used to configure the Token.
  private boolean isAutoBound;
  private boolean isAutoRedeemed;
  private List<String> presentedOfferIDs;       // list of offersIDs presented to the subscriber, in the same order they were presented.
  private String presentedOffersSalesChannel;
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
  public List<String> getScoringStrategyIDs() { return scoringStrategyIDs; }
  public boolean isAutoBound() { return isAutoBound; }
  public boolean isAutoRedeemed() { return isAutoRedeemed; }
  public List<String> getPresentedOfferIDs() { return presentedOfferIDs; }
  public String getPresentedOffersSalesChannel() { return presentedOffersSalesChannel; }
  public String getAcceptedOfferID() { return acceptedOfferID; }

  //
  //  setters
  //

  public void setPresentationStrategyID(String presentationStrategyID) { this.presentationStrategyID = presentationStrategyID; }
  public void setScoringStrategyIDs(List<String> scoringStrategyIDs) { this.scoringStrategyIDs = scoringStrategyIDs; }
  public void setAutoBound(boolean isAutoBound) { this.isAutoBound = isAutoBound; }
  public void setAutoRedeemed(boolean isAutoRedeemed) { this.isAutoRedeemed = isAutoRedeemed; }
  public void setPresentedOfferIDs(List<String> presentedOfferIDs) { this.presentedOfferIDs = presentedOfferIDs; }
  public void setPresentedOffersSalesChannel(String presentedOffersSalesChannel) { this.presentedOffersSalesChannel = presentedOffersSalesChannel; }
  public void setAcceptedOfferID(String acceptedOfferID) { this.acceptedOfferID = acceptedOfferID; }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public DNBOToken(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundedDate,
                   Date redeemedDate, Date tokenExpirationDate, int boundedCount, String eventID,
                   String subscriberID, String tokenTypeID, String moduleID, Integer featureID,
                   String presentationStrategyID, List<String> scoringStrategyIDs, boolean isAutoBound, boolean isAutoRedeemed,
                   List<String> presentedOfferIDs, String presentedOffersSalesChannel, String acceptedOfferID) {
    super(tokenCode, tokenStatus, creationDate, boundedDate, redeemedDate, tokenExpirationDate,
          boundedCount, eventID, subscriberID, tokenTypeID, moduleID, featureID);
    this.presentationStrategyID = presentationStrategyID;
    this.scoringStrategyIDs = scoringStrategyIDs;
    this.isAutoBound = isAutoBound;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.presentedOffersSalesChannel = presentedOffersSalesChannel;
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
         "",                                                              // eventID
         subscriberID,                                                    // subscriberID
         tokenType.getTokenTypeID(),                                      // tokenTypeID
         DeliveryRequest.Module.REST_API.getExternalRepresentation(),     // moduleID
         null,                                                            // featureID
         null,                                                            // presentationStrategyID
         Collections.<String>emptyList(),                                 // scoringStrategyIDs
         false,                                                           // isAutoBound
         false,                                                           // isAutoRedeemed
         new ArrayList<String>(),                                         // presentedOfferIDs
         null,                                                            // presentedOffersSalesChannel
         null);                                                           // acceptedOfferID
  }

  /*****************************************
  *
  * constructor (unpack)
  *
  *****************************************/

  protected DNBOToken(SchemaAndValue schemaAndValue, String presentationStrategyID, List<String> scoringStrategyIDs, boolean isAutoBound, boolean isAutoRedeemed, List<String> presentedOfferIDs, String presentedOffersSalesChannel, String acceptedOfferID)
  {
    super(schemaAndValue);
    this.presentationStrategyID = presentationStrategyID;
    this.scoringStrategyIDs = scoringStrategyIDs;
    this.isAutoBound = isAutoBound;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.presentedOffersSalesChannel = presentedOffersSalesChannel;
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
    struct.put("scoringStrategyIDs",dnboToken.getScoringStrategyIDs());
    struct.put("isAutoBounded", dnboToken.isAutoBound());
    struct.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
    struct.put("presentedOfferIDs", dnboToken.getPresentedOfferIDs());
    struct.put("presentedOffersSalesChannel", dnboToken.getPresentedOffersSalesChannel());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    List<String> scoringStrategyIDs = (schemaVersion >= 2) ? (List<String>) valueStruct.get("scoringStrategyIDs") : Collections.<String>emptyList();
    boolean isAutoBound = valueStruct.getBoolean("isAutoBounded");
    boolean isAutoRedeemed = valueStruct.getBoolean("isAutoRedeemed");
    List<String> presentedOfferIDs = (List<String>) valueStruct.get("presentedOfferIDs");
    String presentedOffersSalesChannel = (schemaVersion >= 3) ? (String) valueStruct.get("presentedOffersSalesChannel") : null;
    String acceptedOfferID = valueStruct.getString("acceptedOfferID");

    //
    // validate
    //

    //
    // return
    //

    return new DNBOToken(schemaAndValue, presentationStrategyID, scoringStrategyIDs, isAutoBound, isAutoRedeemed, presentedOfferIDs, presentedOffersSalesChannel, acceptedOfferID);
  }
}
