/*****************************************************************************
*
*  TokenType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
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
    schemaBuilder.field("validityDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("validityUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("validityRoundUp", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("codeFormat", Schema.STRING_SCHEMA);
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
  private int validityDuration;
  private TimeUnit validityUnit;
  private boolean validityRoundUp;
  private String codeFormat;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTokenTypeID() { return getGUIManagedObjectID(); }
  public String getTokenTypeName() { return getGUIManagedObjectName(); }
  public TokenTypeKind getTokenTypeKind() { return tokenTypeKind; }
  public int getValidityDuration() { return validityDuration; }
  public TimeUnit getValidityUnit() { return validityUnit; }
  public boolean getValidityRoundUp() { return validityRoundUp; }
  public String getCodeFormat() { return codeFormat; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/



  public TokenType(SchemaAndValue schemaAndValue, TokenTypeKind tokenTypeKind, int validityDuration, TimeUnit validityUnit, boolean validityRoundUp, String codeFormat)
  {
    super(schemaAndValue);
    this.tokenTypeKind = tokenTypeKind;
    this.validityDuration = validityDuration;
    this.validityUnit = validityUnit;
    this.validityRoundUp = validityRoundUp;
    this.codeFormat = codeFormat;
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
    struct.put("validityDuration", tokenType.getValidityDuration());
    struct.put("validityUnit", tokenType.getValidityUnit().getExternalRepresentation());
    struct.put("validityRoundUp", tokenType.getValidityRoundUp());
    struct.put("codeFormat", tokenType.getCodeFormat());
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
    int validityDuration = valueStruct.getInt32("validityDuration");
    TimeUnit validityUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("validityUnit"));
    boolean validityRoundUp = valueStruct.getBoolean("validityRoundUp");
    String codeFormat = valueStruct.getString("codeFormat");

    //
    //  return
    //

    return new TokenType(schemaAndValue, tokenTypeKind, validityDuration, validityUnit, validityRoundUp, codeFormat);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TokenType(JSONObject jsonRoot, long epoch, GUIManagedObject existingTokenTypeUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTokenTypeUnchecked != null) ? existingTokenTypeUnchecked.getEpoch() : epoch);

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
    this.validityDuration = JSONUtilities.decodeInteger(jsonRoot, "validityDuration", true);
    this.validityUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityUnit", true));
    this.validityRoundUp = JSONUtilities.decodeBoolean(jsonRoot, "validityRoundUp", Boolean.FALSE);
    this.codeFormat = JSONUtilities.decodeString(jsonRoot, "codeFormat", true);

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
        epochChanged = epochChanged || ! Objects.equals(validityDuration, existingTokenType.getValidityDuration());
        epochChanged = epochChanged || ! Objects.equals(validityUnit, existingTokenType.getValidityUnit());
        epochChanged = epochChanged || ! Objects.equals(validityRoundUp, existingTokenType.getValidityRoundUp());
        epochChanged = epochChanged || ! Objects.equals(codeFormat, existingTokenType.getCodeFormat());
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
  *****************************************/
  
  public TokenTypeKind getKind() {
    return TokenTypeKind.Unknown; // Should be overridden by subclasses
  }
}
