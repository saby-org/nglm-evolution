/*****************************************************************************
*
*  TokenType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class TokenType extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum TokenTypeKind
  {
    OffersPresentation("offersPresentation"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private TokenTypeKind(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static TokenTypeKind fromExternalRepresentation(String externalRepresentation) { for (TokenTypeKind enumeratedValue : TokenTypeKind.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.name("token_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("tokenTypeKind", Schema.STRING_SCHEMA);
    schemaBuilder.field("validity", TokenTypeValidity.schema());
    schemaBuilder.field("codeFormat", Schema.STRING_SCHEMA);
    schemaBuilder.field("maxNumberOfPlays", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<TokenType> serde = new ConnectSerde<TokenType>(schema, false, TokenType.class, TokenType::pack, TokenType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<TokenType> serde() { return serde; }

 /*****************************************
  *
  *  data
  *
  *****************************************/

  private TokenTypeKind tokenTypeKind;
  private TokenTypeValidity validity;
  private String codeFormat;
  private Integer maxNumberOfPlays;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTokenTypeID() { return getGUIManagedObjectID(); }
  public String getTokenTypeName() { return getGUIManagedObjectName(); }
  public String getTokenTypeDisplay() { return getGUIManagedObjectDisplay(); }
  public TokenTypeKind getTokenTypeKind() { return tokenTypeKind; }
  public TokenTypeValidity getValidity() { return validity; }
  public String getCodeFormat() { return codeFormat; }
  public Integer getMaxNumberOfPlays() { return maxNumberOfPlays; }

  //
  // getExpirationDate
  //

  // TODO: RoundDown has not been implemented yet.
  public Date getExpirationDate(Date creationDate) {    
    Date result = null;
    switch(this.validity.getPeriodType()) {
    case Minute:
      result = RLMDateUtils.addMinutes(creationDate, this.validity.getPeriodQuantity());
      break;
    case Hour:
      result = RLMDateUtils.addHours(creationDate, this.validity.getPeriodQuantity());
      break;
    case Day:
      result = RLMDateUtils.addDays(creationDate, this.validity.getPeriodQuantity(), Deployment.getDeployment(tenantID).getBaseTimeZone());
      break;
    case Week:
      result = RLMDateUtils.addWeeks(creationDate, this.validity.getPeriodQuantity(), Deployment.getDeployment(tenantID).getBaseTimeZone());
      break;
    case Month:
      result = RLMDateUtils.addMonths(creationDate, this.validity.getPeriodQuantity(), Deployment.getDeployment(tenantID).getBaseTimeZone());
      break;
    case Year:
      result = RLMDateUtils.addYears(creationDate, this.validity.getPeriodQuantity(), Deployment.getDeployment(tenantID).getBaseTimeZone());
      break;
    default:
    }

    return result;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public TokenType(SchemaAndValue schemaAndValue, TokenTypeKind tokenTypeKind, TokenTypeValidity validity, String codeFormat, Integer maxNumberOfPlays)
  {
    super(schemaAndValue);
    this.tokenTypeKind = tokenTypeKind;
    this.validity = validity;
    this.codeFormat = codeFormat;
    this.maxNumberOfPlays = maxNumberOfPlays;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    TokenType tokenType = (TokenType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, tokenType);
    struct.put("tokenTypeKind", tokenType.getTokenTypeKind().getExternalRepresentation());
    struct.put("validity", TokenTypeValidity.pack(tokenType.getValidity()));
    struct.put("codeFormat", tokenType.getCodeFormat());
    struct.put("maxNumberOfPlays", tokenType.getMaxNumberOfPlays());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static TokenType unpack(SchemaAndValue schemaAndValue)
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
    TokenTypeKind tokenTypeKind = TokenTypeKind.fromExternalRepresentation(valueStruct.getString("tokenTypeKind"));
    TokenTypeValidity validity = TokenTypeValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));
    String codeFormat = valueStruct.getString("codeFormat");
    Integer maxNumberOfPlays = valueStruct.getInt32("maxNumberOfPlays");

    //
    //  return
    //

    return new TokenType(schemaAndValue, tokenTypeKind, validity, codeFormat, maxNumberOfPlays);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TokenType(JSONObject jsonRoot, long epoch, GUIManagedObject existingTokenTypeUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTokenTypeUnchecked != null) ? existingTokenTypeUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingTokenType
    *
    *****************************************/

    TokenType existingTokenType = (existingTokenTypeUnchecked != null && existingTokenTypeUnchecked instanceof TokenType) ? (TokenType) existingTokenTypeUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.tokenTypeKind = TokenTypeKind.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "tokenTypeKind", true));
    this.validity = new TokenTypeValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity"));
    this.codeFormat = JSONUtilities.decodeString(jsonRoot, "codeFormat", true);
    this.maxNumberOfPlays = JSONUtilities.decodeInteger(jsonRoot, "maxNumberOfPlays", false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingTokenType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(TokenType existingTokenType)
  {
    if (existingTokenType != null && existingTokenType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingTokenType.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(tokenTypeKind, existingTokenType.getTokenTypeKind());
        epochChanged = epochChanged || ! Objects.equals(validity, existingTokenType.getValidity());
        epochChanged = epochChanged || ! Objects.equals(codeFormat, existingTokenType.getCodeFormat());
        epochChanged = epochChanged || ! Objects.equals(maxNumberOfPlays, existingTokenType.getMaxNumberOfPlays());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  getKind
  *
  *  TODO: getKind is redundant with getTokenTypeKind, plus it does not work ATM because token types from TokenTypeService are not ser/de in their specific implementation but as an abstract TokenType
  *
  *****************************************/
  /*
  public TokenTypeKind getKind() {
    return TokenTypeKind.Unknown; // Should be overridden by subclasses
  }*/
}
