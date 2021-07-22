/****************************************************************************
*
*  DNBOTokenType.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.SchemaAndValue;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "dnbotokentype", serviceClass = TokenTypeService.class, dependencies = { })
public class DNBOTokenType extends TokenType {

  public DNBOTokenType(SchemaAndValue schemaAndValue, TokenTypeValidity validity, String codeFormat, Integer maxNumberOfPlays) {
    super(schemaAndValue, TokenTypeKind.OffersPresentation, validity, codeFormat, maxNumberOfPlays);
  }

  //
  // getKind is redundant with getTokenTypeKind, plus it does not work ATM because token types from TokenTypeService are not Serde in their specific implementation but as an abstract TokenType
  //

  /*
  @Override
  public TokenTypeKind getKind() {
    return TokenTypeKind.OffersPresentation;
  }*/
}
