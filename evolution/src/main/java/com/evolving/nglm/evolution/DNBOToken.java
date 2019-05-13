/****************************************************************************
*
*  DNBOToken.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.Token;
import com.evolving.nglm.evolution.Token.TokenStatus;

public class DNBOToken extends Token {

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
    schemaBuilder.name("dnbo_token");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("isAutoBounded", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("isAutoRedeemed", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("presentedOffersIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("acceptedOfferID", Schema.OPTIONAL_STRING_SCHEMA); 
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<DNBOToken> serde = new ConnectSerde<DNBOToken>(schema, false, DNBOToken.class, DNBOToken::pack, DNBOToken::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOToken> serde() { return serde; }
  
  /****************************************
  *
  *  data
  *
  ****************************************/

  /**
   * This field stores a reference to the {@link PresentationStrategy} used to configure the Token.
   */
  private String presentationStrategyID;

  /**
   * This field is {@code true} in case the token has been auto-bounded. {@code false} otherwise.
   */
  private boolean isAutoBounded;
 
  /**
   * This field is {@code true} in case the token has been auto-redeemed. {@code false} otherwise.
   */
  private boolean isAutoRedeemed;

  /**
   * This field stores a list of offersIDs presented to the subscriber, in the order they were presented.
   */
  private List<String> presentedOffersIDs;

  /**
   * This field stores the offer that has been accepted (if any).
   */
  private String acceptedOfferID;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  //
  //  accessor
  //

  
  /**
   * @return the presentationStrategyID
   */
  public String getPresentationStrategyID() {
    return presentationStrategyID;
  }

  /**
   * @return the isAutoBounded
   */
  public boolean isAutoBounded() {
    return isAutoBounded;
  }

  /**
   * @return the isAutoRedeemed
   */
  public boolean isAutoRedeemed() {
    return isAutoRedeemed;
  }

  /**
   * @return the presentedOffersIDs
   */
  public List<String> getPresentedOffersIDs() {
    return presentedOffersIDs;
  }

  /**
   * @return the acceptedOfferID
   */
  public String getAcceptedOfferID() {
    return acceptedOfferID;
  }
  
  
  
  /**
   * @param presentationStrategyID the presentationStrategyID to set
   */
  public void setPresentationStrategyID(String presentationStrategyID) {
    this.presentationStrategyID = presentationStrategyID;
  }

  /**
   * @param isAutoBounded the isAutoBounded to set
   */
  public void setAutoBounded(boolean isAutoBounded) {
    this.isAutoBounded = isAutoBounded;
  }

  /**
   * @param isAutoRedeemed the isAutoRedeemed to set
   */
  public void setAutoRedeemed(boolean isAutoRedeemed) {
    this.isAutoRedeemed = isAutoRedeemed;
  }

  /**
   * @param presentedOffersIDs the presentedOffersIDs to set
   */
  public void setPresentedOffersIDs(List<String> presentedOffersIDs) {
    this.presentedOffersIDs = presentedOffersIDs;
  }

  /**
   * @param acceptedOfferID the acceptedOfferID to set
   */
  public void setAcceptedOfferID(String acceptedOfferID) {
    this.acceptedOfferID = acceptedOfferID;
  }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public DNBOToken(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundedDate,
      Date redeemedDate, Date tokenExpirationDate, int boundedCount, String eventID,
      String subscriberID, String tokenTypeID, int moduleID, int featureID,
      String presentationStrategyID, boolean isAutoBounded, boolean isAutoRedeemed,
      List<String> presentedOffersIDs, String acceptedOfferID) {
    super(tokenCode, tokenStatus, creationDate, boundedDate, redeemedDate, tokenExpirationDate,
        boundedCount, eventID, subscriberID, tokenTypeID, moduleID, featureID);
    this.presentationStrategyID = presentationStrategyID;
    this.isAutoBounded = isAutoBounded;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOffersIDs = presentedOffersIDs;
    this.acceptedOfferID = acceptedOfferID;
  }
  
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  protected DNBOToken(SchemaAndValue schemaAndValue, String presentationStrategyID, boolean isAutoBounded, boolean isAutoRedeemed,
      List<String> presentedOffersIDs, String acceptedOfferID)
  {
    super(schemaAndValue);
    this.presentationStrategyID = presentationStrategyID;
    this.isAutoBounded = isAutoBounded;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOffersIDs = presentedOffersIDs;
    this.acceptedOfferID = acceptedOfferID;

  }

  /*****************************************
  *
  *  pack
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
    struct.put("presentedOffersIDs", dnboToken.getPresentedOffersIDs());
    struct.put("acceptedOfferID", dnboToken.getAcceptedOfferID());
    return struct;
  }
}
