/****************************************************************************
*
*  ReferenceDataValue.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.util.Date;

public interface ReferenceDataValue<K>
{
  public K getKey();
  default public boolean getDeleted() { return false; }
  default public Date getEffectiveStartDate() { return NGLMRuntime.BEGINNING_OF_TIME; }
  default public Date getEffectiveEndDate() { return NGLMRuntime.END_OF_TIME; }
}
