/****************************************************************************
*
*  Token.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;

public class Token implements Action
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum TokenStatus
  {
    New("new"),
    Bound("bound"),
    Redeemed("redeemed"),
    Expired("expired"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private TokenStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static TokenStatus fromExternalRepresentation(String externalRepresentation) { for (TokenStatus enumeratedValue : TokenStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("token");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("tokenCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("creationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("boundDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("redeemedDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("tokenExpirationDate", Timestamp.builder().schema());
    schemaBuilder.field("boundCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<Token> commonSerde;
  static
  {
    List<ConnectSerde<? extends Token>> tokenSerdes = new ArrayList<ConnectSerde<? extends Token>>();
    tokenSerdes.add(DNBOToken.serde());
    commonSerde = new ConnectSerde<Token>("token", false, tokenSerdes.toArray(new ConnectSerde[0]));
  }

  //
  // accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<Token> commonSerde() { return commonSerde; }

  /*****************************************
  *
  * data
  *
  *****************************************/

  private String tokenCode;
  private TokenStatus tokenStatus;
  private Date creationDate;
  private Date boundDate;                   // last time it was bound
  private Date redeemedDate;
  private Date tokenExpirationDate;
  private int boundCount;                   // how many time the token has been bound
  private String eventID;                   // reference to the TokenManagerEvent that generated this token
  private String subscriberID;
  private String tokenTypeID;
  private String moduleID;                  // reference to the Token requester
  private String featureID;                // reference to the Token requester

  /*****************************************
  *
  * accessors
  *
  *****************************************/

  public String getTokenCode() { return tokenCode; }
  public TokenStatus getTokenStatus() { return tokenStatus; }
  public Date getCreationDate() { return creationDate; }
  public Date getBoundDate() { return boundDate; }
  public Date getRedeemedDate() { return redeemedDate; }
  public Date getTokenExpirationDate() { return tokenExpirationDate; }
  public int getBoundCount() { return boundCount; }
  public String getEventID() { return eventID; }
  public String getSubscriberID() { return subscriberID; }
  public String getTokenTypeID() { return tokenTypeID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public ActionType getActionType() { return ActionType.TokenUpdate; }

  //
  //  setters
  //

  public void setTokenCode(String tokenCode) { this.tokenCode = tokenCode; }
  public void setTokenStatus(TokenStatus tokenStatus) { this.tokenStatus = tokenStatus; }
  public void setCreationDate(Date creationDate) { this.creationDate = creationDate; }
  public void setBoundDate(Date boundDate) { this.boundDate = boundDate; }
  public void setRedeemedDate(Date redeemedDate) { this.redeemedDate = redeemedDate; }
  public void setTokenExpirationDate(Date tokenExpirationDate) { this.tokenExpirationDate = tokenExpirationDate; }
  public void setBoundCount(int boundCount) { this.boundCount = boundCount; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void setTokenTypeID(String tokenTypeID) { this.tokenTypeID = tokenTypeID; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setFeatureID(String featureID) { this.featureID = featureID; }

  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public Token(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundDate, Date redeemedDate, Date tokenExpirationDate, int boundCount, String eventID, String subscriberID, String tokenTypeID, String moduleID, String featureID)
  {
    this.tokenCode = tokenCode;
    this.tokenStatus = tokenStatus;
    this.creationDate = creationDate;
    this.boundDate = boundDate;
    this.redeemedDate = redeemedDate;
    this.tokenExpirationDate = tokenExpirationDate;
    this.boundCount = boundCount;
    this.eventID = eventID;
    this.subscriberID = subscriberID;
    this.tokenTypeID = tokenTypeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
  }

  /*****************************************
  *
  * packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, Token token)
  {
    struct.put("tokenCode", token.getTokenCode());
    struct.put("tokenStatus", token.getTokenStatus().getExternalRepresentation());
    struct.put("creationDate", token.getCreationDate());
    struct.put("boundDate", token.getBoundDate());
    struct.put("redeemedDate", token.getRedeemedDate());
    struct.put("tokenExpirationDate", token.getTokenExpirationDate());
    struct.put("boundCount", token.getBoundCount());
    struct.put("eventID", token.getEventID());
    struct.put("subscriberID", token.getSubscriberID());
    struct.put("tokenTypeID", token.getTokenTypeID());
    struct.put("moduleID", token.getModuleID());
    struct.put("featureID", token.getFeatureID());
  }

  /*****************************************
  *
  * constructor (unpack)
  *
  *****************************************/

  protected Token(SchemaAndValue schemaAndValue)
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
    String tokenCode = valueStruct.getString("tokenCode");
    TokenStatus tokenStatus = TokenStatus.fromExternalRepresentation(valueStruct.getString("tokenStatus"));
    Date creationDate = (Date) valueStruct.get("creationDate");
    Date boundDate = (Date) valueStruct.get("boundDate");
    Date redeemedDate = (Date) valueStruct.get("redeemedDate");
    Date tokenExpirationDate = (Date) valueStruct.get("tokenExpirationDate");
    int boundCount = valueStruct.getInt32("boundCount");
    String eventID = valueStruct.getString("eventID");
    String subscriberID = valueStruct.getString("subscriberID");
    String tokenTypeID = valueStruct.getString("tokenTypeID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = (schemaVersion >=2 ) ? valueStruct.getString("featureID") : "";

    //
    //  return
    //

    this.tokenCode = tokenCode;
    this.tokenStatus = tokenStatus;
    this.creationDate = creationDate;
    this.boundDate = boundDate;
    this.redeemedDate = redeemedDate;
    this.tokenExpirationDate = tokenExpirationDate;
    this.boundCount = boundCount;
    this.eventID = eventID;
    this.subscriberID = subscriberID;
    this.tokenTypeID = tokenTypeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.subscriberID = subscriberID;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override public String toString()
  {
    return
        "Token [" + (tokenCode != null ? "tokenCode=" + tokenCode + ", " : "")
        + (tokenStatus != null ? "tokenStatus=" + tokenStatus + ", " : "")
        + (creationDate != null ? "creationDate=" + creationDate + ", " : "")
        + (boundDate != null ? "boundDate=" + boundDate + ", " : "")
        + (redeemedDate != null ? "redeemedDate=" + redeemedDate + ", " : "")
        + (tokenExpirationDate != null ? "tokenExpirationDate=" + tokenExpirationDate + ", " : "")
        + "boundCount=" + boundCount + ", "
        + (eventID != null ? "eventID=" + eventID + ", " : "")
        + (subscriberID != null ? "subscriberID=" + subscriberID + ", " : "")
        + (tokenTypeID != null ? "tokenTypeID=" + tokenTypeID + ", " : "") + "moduleID=" + moduleID
        + ", featureID=" + featureID + "]";
  }
}
