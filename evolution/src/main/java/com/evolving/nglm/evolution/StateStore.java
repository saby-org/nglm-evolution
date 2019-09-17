/*****************************************
*
*  StateStore.java
*
*****************************************/

package com.evolving.nglm.evolution;

public interface StateStore
{
  public void setKafkaRepresentation(byte[] representation);
  public byte[] getKafkaRepresentation();
}
