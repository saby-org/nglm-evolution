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
}
