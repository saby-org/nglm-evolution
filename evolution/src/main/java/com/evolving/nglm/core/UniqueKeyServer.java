/****************************************************************************
*
*  UniqueKeyServer.java
*
*
*  Copyright 2000-2009 RateIntegration, Inc.  All Rights Reserved.
*
****************************************************************************/

package com.evolving.nglm.core;

/**
 * UniqueKeyServer provides a simple mechanism to produce per-instance unique
 * identifiers.
 */
public class UniqueKeyServer
{
  private long epoch = System.currentTimeMillis() * 1000;
  private long counter = epoch;

  /**
   * Returns a long that is guaranteed to be different than any other key yet
   * produced by this UniqueKeyServer instance. Note that multiple instances
   * of UniqueKeyServer <b>will not</b> necessarily produce keys that are unique
   * across instances. 
   * @return A long that is guaranteed to be unique per instance.
   */
  public synchronized long getKey()
  {
    counter += 1;
    return counter;
  }

  /**
   * Returns the initial value of the UniqueKeyServer.
   * @return A long that is the initial value of the UniqueKeyServer
   */
  public long getEpoch()
  {
    return epoch;
  }
}
