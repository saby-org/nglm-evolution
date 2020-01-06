package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

public class MeansOfPaymentDisplayMapping extends DisplayMapping<MeanOfPaymentInformation>
{
  public static final String ESIndex = "mapping_paymentmeans";
  
  public MeansOfPaymentDisplayMapping() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("paymentMeanID"), new MeanOfPaymentInformation((String) row.get("paymentMeanName"), (String) row.get("paymentMeanProviderID")));
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/
  
  public String getDisplay(String id)
  {
    MeanOfPaymentInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.meanOfPaymentDisplay;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve meanOfPayment.display and meanOfPayment.providerID for meanOfPayment.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
  
  public String getProviderID(String id)
  {
    MeanOfPaymentInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.meanOfPaymentProviderID;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve meanOfPayment.display and meanOfPayment.providerID for meanOfPayment.id: " + id);
        return "provider_Point"; // When missing, return "provider_Point" by default.
      }
  }
}
