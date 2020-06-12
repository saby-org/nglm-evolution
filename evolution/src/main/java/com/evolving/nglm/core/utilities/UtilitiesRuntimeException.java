/*****************************************************************************
*
*  UtilitiesRuntimeException.java
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

public class UtilitiesRuntimeException extends RuntimeException
{
  public UtilitiesRuntimeException(String message) { super(message); }
  public UtilitiesRuntimeException(Throwable e) { super(e); }
  public UtilitiesRuntimeException(String message, Throwable e) { super(message,e); }
}
