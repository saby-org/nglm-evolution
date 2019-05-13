/****************************************************************************
*
*  DNBOTokenType.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.SchemaAndValue;

import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.TokenType.TokenTypeKind;

public class DNBOTokenType extends TokenType {

  public DNBOTokenType(SchemaAndValue schemaAndValue, TokenTypeKind tokenTypeKind,
      int validityDuration, TimeUnit validityUnit, boolean validityRoundUp, String codeFormat) {
    super(schemaAndValue, tokenTypeKind, validityDuration, validityUnit, validityRoundUp, codeFormat);
  }

  @Override
  public TokenTypeKind getKind() {
    return TokenTypeKind.OffersPresentation;
  }
}
