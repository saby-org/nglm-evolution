/****************************************************************************
*
*  DNBOTokenType.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.SchemaAndValue;

public class DNBOTokenType extends TokenType {

  public DNBOTokenType(SchemaAndValue schemaAndValue, TokenTypeKind tokenTypeKind,
      TokenTypeValidity validity, String codeFormat,Integer maxNumberOfPlays) {
    super(schemaAndValue, tokenTypeKind, validity, codeFormat, maxNumberOfPlays);
  }

  @Override
  public TokenTypeKind getKind() {
    return TokenTypeKind.OffersPresentation;
  }
}
