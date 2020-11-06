package com.evolving.nglm.evolution;

import java.util.Date;

public interface VoucherRedemption
{
  //
  //  standard voucher action accessors
  //

  default public String getVoucherRedemptionSubscriberID() { return ((VoucherAction) this).getSubscriberID(); }
  default public Date getVoucherRedemptionEventDate() { return ((VoucherAction) this).getEventDate(); }
  default public String getVoucherRedemptionVoucherCode() { return ((VoucherAction) this).getVoucherCode(); }
  default public String getVoucherRedemptionStatus() { return ((VoucherAction) this).getActionStatus(); }
  default public Integer getVoucherRedemptionStatusCode() { return ((VoucherAction) this).getActionStatusCode(); }

}
