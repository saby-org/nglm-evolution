/*****************************************************************************
*
*  ServerRuntimeException
*
*****************************************************************************/

package com.evolving.nglm.core;

public class ServerRuntimeException extends RuntimeException
{
  public ServerRuntimeException(String message) { super(message); }
  public ServerRuntimeException(Throwable e) { super(e); }
  public ServerRuntimeException(String message, Throwable e) { super(message,e); }
}
