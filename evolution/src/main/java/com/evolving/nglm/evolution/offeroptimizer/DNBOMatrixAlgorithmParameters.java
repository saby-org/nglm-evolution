package com.evolving.nglm.evolution.offeroptimizer;

import com.evolving.nglm.evolution.DNBOMatrixService;

/**
 * This class is used only to encapsulate parameters needed for DNBO matrix processing by implement IOfferOptimizerAlgorithm
 * clasa asta ar trebui sa fie single ton cu set doar pe range, dar vedem
 * @author sbartha
 *
 */
public class DNBOMatrixAlgorithmParameters
{

  /*****************************************
   *
   *  data
   *
   *****************************************/

  private DNBOMatrixService dnboMatrixService;
  private double dnboRangeValue;

  /*****************************************
   *
   *  constructor
   *
   *****************************************/

  public DNBOMatrixAlgorithmParameters(DNBOMatrixService dnboMatrixService,double dnboRangeValue)
  {
    this.dnboMatrixService = dnboMatrixService;
    this.dnboRangeValue = dnboRangeValue;
  }

  /*****************************************
   *
   *  getDnboMatrixService
   *
   *****************************************/

  public DNBOMatrixService getDnboMatrixService()
  {
    return dnboMatrixService;
  }

  /*****************************************
   *
   *  getDnboRangeValue
   *
   *****************************************/

  public double getDnboRangeValue()
  {
    return dnboRangeValue;
  }
}
