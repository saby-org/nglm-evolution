/*****************************************************************************
*
*  UtilitiesException.java
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

public class UtilitiesException extends Exception
{
  public UtilitiesException(String message) { super(message); }
  public UtilitiesException(Throwable e) { super(e); }
  public UtilitiesException(String message, Throwable e) { super(message,e); }
}
