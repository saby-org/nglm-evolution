package com.evolving.nglm.evolution;

import java.util.Date;

public interface VoucherValidation
{
  //
  //  standard voucher action accessors
  //

  default public String getVoucherValidationSubscriberID() { return ((VoucherAction) this).getSubscriberID(); }
  default public Date getVoucherValidationEventDate() { return ((VoucherAction) this).getEventDate(); }
  default public String getVoucherValidationVoucherCode() { return ((VoucherAction) this).getVoucherCode(); }
  default public String getVoucherValidationStatus() { return ((VoucherAction) this).getActionStatus(); }
  default public Integer getVoucherValidationStatusCode() { return ((VoucherAction) this).getActionStatusCode(); };

}
