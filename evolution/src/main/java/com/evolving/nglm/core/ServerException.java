/*****************************************************************************
*
*  ServerException
*
*****************************************************************************/

package com.evolving.nglm.core;

public class ServerException extends Exception
{
  public ServerException(String message) { super(message); }
  public ServerException(Throwable e) { super(e); }
  public ServerException(String message, Throwable e) { super(message,e); }
}
