/*****************************************************************************
*
*  Pair.java
*
*
*  Copyright 2000-2012 RateIntegration, Inc.  All Rights Reserved.
*
*****************************************************************************/

package com.evolving.nglm.core;

public class Pair<A,B>
{
  //
  //  instance data
  //
  
  private A firstElement;
  private B secondElement;

  //
  //  accessors
  //
  
  public A getFirstElement() { return firstElement; }
  public B getSecondElement() { return secondElement; }
  
  //
  //  constructor
  //
  
  public Pair(A firstElement, B secondElement)
  {
    this.firstElement = firstElement;
    this.secondElement = secondElement;
  }

  //
  //  equals
  //

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof Pair)
      {
        Pair pair = (Pair) obj;
        result = firstElement.equals(pair.getFirstElement()) && secondElement.equals(pair.getSecondElement());
      }
    return result;
  }

  //
  //  hashcode
  //
  
  public int hashCode()
  {
    return firstElement.hashCode() + secondElement.hashCode();
  }

  //
  //  toString
  //
  
  public String toString()
  {
    return new String("<" + firstElement + "," + secondElement + ">");
  }
}
