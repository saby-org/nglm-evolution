/****************************************************************************
*
*  Token.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class Token {
  
  public enum TokenStatus
  {
    New("new"),
    Bounded("bounded"),
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("tokenCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("creationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("boundedDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("redeemedDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("tokenExpirationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("boundedCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA); // TODO: Optional? 
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenTypeID", Schema.OPTIONAL_STRING_SCHEMA); // TODO: Optional? 
    schemaBuilder.field("moduleID", Schema.OPTIONAL_INT32_SCHEMA); // TODO: Optional? 
    schemaBuilder.field("featureID", Schema.OPTIONAL_INT32_SCHEMA); // TODO: Optional? 
    commonSchema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Token> serde = new ConnectSerde<Token>(commonSchema, false, Token.class, Token::pack, Token::unpack);

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<Token> commonSerde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  /**
   * This field stores the token code.
   */
  private String tokenCode;

  /**
   * This field stores the status of this token.
   */
  private TokenStatus tokenStatus;

  /**
   * This field stores when this token was created.
   */
  private Date creationDate;
  
  /**
   * This field stores the last use time.
   */
  private Date boundedDate;

  /**
   * This field stores when this token has been redeemed.
   */
  private Date redeemedDate;

  /**
   * This field stores when this token will expire. A null value means no expiration.
   */
  private Date tokenExpirationDate;

  /**
   * This field stores how many times this token has been bounded.
   */
  private int boundedCount;
  
  /**
   * This field stores a reference to the TokenManagerEvent that generated this Token.
   */
  private String eventID;

  /**
   * This field stores a reference to the Subscriber associated to this Token.
   */
  private String subscriberID;

  /**
   * This field stores the tokenType related to this token.
   */
  private String tokenTypeID;

  /**
   * This field stores a reference to the Token requester.
   */
  private Integer moduleID;

  /**
   * This field stores a reference to the Token requester.
   */
  private Integer featureID;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public Token(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundedDate,
      Date redeemedDate, Date tokenExpirationDate, int boundedCount, String eventID,
      String subscriberID, String tokenTypeID, Integer moduleID, Integer featureID) {
    this.tokenCode = tokenCode;
    this.tokenStatus = tokenStatus;
    this.creationDate = creationDate;
    this.boundedDate = boundedDate;
    this.redeemedDate = redeemedDate;
    this.tokenExpirationDate = tokenExpirationDate;
    this.boundedCount = boundedCount;
    this.eventID = eventID;
    this.subscriberID = subscriberID;
    this.tokenTypeID = tokenTypeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
  }
  
  /**
   * @return the tokenCode
   */
  public String getTokenCode() {
    return tokenCode;
  }
  /**
   * @return the tokenStatus
   */
  public TokenStatus getTokenStatus() {
    return tokenStatus;
  }
  /**
   * @return the creationDate
   */
  public Date getCreationDate() {
    return creationDate;
  }
  /**
   * @return the boundedDate
   */
  public Date getBoundedDate() {
    return boundedDate;
  }
  /**
   * @return the redeemedDate
   */
  public Date getRedeemedDate() {
    return redeemedDate;
  }
  /**
   * @return the tokenExpirationDate
   */
  public Date getTokenExpirationDate() {
    return tokenExpirationDate;
  }
  /**
   * @return the boundedCount
   */
  public int getBoundedCount() {
    return boundedCount;
  }
  /**
   * @return the eventID
   */
  public String getEventID() {
    return eventID;
  }
  /**
   * @return the subscriberID
   */
  public String getSubscriberID() {
    return subscriberID;
  }
  /**
   * @return the tokenTypeID
   */
  public String getTokenTypeID() {
    return tokenTypeID;
  }
  /**
   * @return the moduleID
   */
  public Integer getModuleID() {
    return moduleID;
  }
  /**
   * @return the featureID
   */
  public Integer getFeatureID() {
    return featureID;
  }
  
  /**
   * @param tokenCode the tokenCode to set
   */
  public void setTokenCode(String tokenCode) {
    this.tokenCode = tokenCode;
  }
  /**
   * @param tokenStatus the tokenStatus to set
   */
  public void setTokenStatus(TokenStatus tokenStatus) {
    this.tokenStatus = tokenStatus;
  }
  /**
   * @param creationDate the creationDate to set
   */
  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }
  /**
   * @param boundedDate the boundedDate to set
   */
  public void setBoundedDate(Date boundedDate) {
    this.boundedDate = boundedDate;
  }
  /**
   * @param redeemedDate the redeemedDate to set
   */
  public void setRedeemedDate(Date redeemedDate) {
    this.redeemedDate = redeemedDate;
  }
  /**
   * @param tokenExpirationDate the tokenExpirationDate to set
   */
  public void setTokenExpirationDate(Date tokenExpirationDate) {
    this.tokenExpirationDate = tokenExpirationDate;
  }
  /**
   * @param boundedCount the boundedCount to set
   */
  public void setBoundedCount(int boundedCount) {
    this.boundedCount = boundedCount;
  }
  /**
   * @param eventID the eventID to set
   */
  public void setEventID(String eventID) {
    this.eventID = eventID;
  }
  /**
   * @param subscriberID the subscriberID to set
   */
  public void setSubscriberID(String subscriberID) {
    this.subscriberID = subscriberID;
  }
  /**
   * @param tokenTypeID the tokenTypeID to set
   */
  public void setTokenTypeID(String tokenTypeID) {
    this.tokenTypeID = tokenTypeID;
  }
  /**
   * @param moduleID the moduleID to set
   */
  public void setModuleID(Integer moduleID) {
    this.moduleID = moduleID;
  }
  /**
   * @param featureID the featureID to set
   */
  public void setFeatureID(Integer featureID) {
    this.featureID = featureID;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Token token = (Token) value;
    Struct struct = new Struct(commonSchema);
    struct.put("tokenCode", token.getTokenCode());
    struct.put("tokenStatus", token.getTokenStatus());
    struct.put("creationDate", token.getCreationDate());
    struct.put("boundedDate", token.getBoundedDate());
    struct.put("redeemedDate", token.getRedeemedDate());
    struct.put("tokenExpirationDate", token.getTokenExpirationDate());
    struct.put("boundedCount", token.getBoundedCount());
    struct.put("eventID", token.getEventID());
    struct.put("subscriberID", token.getSubscriberID());
    struct.put("tokenTypeID", token.getTokenTypeID());
    struct.put("moduleID", token.getModuleID());
    struct.put("featureID", token.getFeatureID());
    return struct;
  }


  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void packCommon(Struct struct, Token token)
  {
    struct.put("tokenCode", token.getTokenCode());
    struct.put("tokenStatus", token.getTokenStatus());
    struct.put("creationDate", token.getCreationDate());
    struct.put("boundedDate", token.getBoundedDate());
    struct.put("redeemedDate", token.getRedeemedDate());
    struct.put("tokenExpirationDate", token.getTokenExpirationDate());
    struct.put("boundedCount", token.getBoundedCount());
    struct.put("eventID", token.getEventID());
    struct.put("subscriberID", token.getSubscriberID());
    struct.put("tokenTypeID", token.getTokenTypeID());
    struct.put("moduleID", token.getModuleID());
    struct.put("featureID", token.getFeatureID());
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Token unpack(SchemaAndValue schemaAndValue)
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
    Date boundedDate = (Date) valueStruct.get("boundedDate");
    Date redeemedDate = (Date) valueStruct.get("redeemedDate");
    Date tokenExpirationDate = (Date) valueStruct.get("tokenExpirationDate");
    int boundedCount = valueStruct.getInt32("boundedCount");
    String eventID = valueStruct.getString("eventID");
    String subscriberID = valueStruct.getString("subscriberID");
    String tokenTypeID = valueStruct.getString("tokenTypeID");
    int moduleID = valueStruct.getInt32("moduleID");
    int featureID = valueStruct.getInt32("featureID");
    
    //
    //  validate
    //
 
    //
    //  return
    //

    return new Token(tokenCode, tokenStatus, creationDate, boundedDate,
        redeemedDate, tokenExpirationDate, boundedCount, eventID,
        subscriberID, tokenTypeID, moduleID, featureID);
  }


  /*****************************************
  *
  *  constructor (unpack)
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
    Date boundedDate = (Date) valueStruct.get("boundedDate");
    Date redeemedDate = (Date) valueStruct.get("redeemedDate");
    Date tokenExpirationDate = (Date) valueStruct.get("tokenExpirationDate");
    int boundedCount = valueStruct.getInt32("boundedCount");
    String eventID = valueStruct.getString("eventID");
    String subscriberID = valueStruct.getString("subscriberID");
    String tokenTypeID = valueStruct.getString("tokenTypeID");
    Integer moduleID = valueStruct.getInt32("moduleID");
    Integer featureID = valueStruct.getInt32("featureID");

    //
    //  return
    //
    
    this.tokenCode = tokenCode;
    this.tokenStatus = tokenStatus;
    this.creationDate = creationDate;
    this.boundedDate = boundedDate;
    this.redeemedDate = redeemedDate;
    this.tokenExpirationDate = tokenExpirationDate;
    this.boundedCount = boundedCount;
    this.eventID = eventID;
    this.subscriberID = subscriberID;
    this.tokenTypeID = tokenTypeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.subscriberID = subscriberID;
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Token [" + (tokenCode != null ? "tokenCode=" + tokenCode + ", " : "")
        + (tokenStatus != null ? "tokenStatus=" + tokenStatus + ", " : "")
        + (creationDate != null ? "creationDate=" + creationDate + ", " : "")
        + (boundedDate != null ? "boundedDate=" + boundedDate + ", " : "")
        + (redeemedDate != null ? "redeemedDate=" + redeemedDate + ", " : "")
        + (tokenExpirationDate != null ? "tokenExpirationDate=" + tokenExpirationDate + ", " : "")
        + "boundedCount=" + boundedCount + ", "
        + (eventID != null ? "eventID=" + eventID + ", " : "")
        + (subscriberID != null ? "subscriberID=" + subscriberID + ", " : "")
        + (tokenTypeID != null ? "tokenTypeID=" + tokenTypeID + ", " : "") + "moduleID=" + moduleID
        + ", featureID=" + featureID + "]";
  }
  
  
}
