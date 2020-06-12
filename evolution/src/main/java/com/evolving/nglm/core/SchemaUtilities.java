/*****************************************************************************
*
*  SchemaUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.core;

public class SchemaUtilities
{
  /*****************************************
  *
  *  packSchemaVersion
  *
  *****************************************/

  //
  //  simple
  //

  public static int packSchemaVersion(int version) { return packSchemaVersion(0, version); }

  //
  //  parent 
  //
  
  public static int packSchemaVersion(int parentVersion, int version)
  {
    //
    //  validate
    //
    
    if (parentVersion > 0x7FFFFF) throw new ServerRuntimeException("illegal parent version");
    if (version > 0x7F) throw new ServerRuntimeException("illegal version");
    
    //
    //  schema version
    //
    
    if ((parentVersion & 0x7F) == 0)
      version = parentVersion | (version << 0);
    else if ((parentVersion & 0x7F00) == 0)
      version = parentVersion | (version << 8);
    else if ((parentVersion & 0x7F0000) == 0)
      version = parentVersion | (version << 16);
    else
      version = parentVersion | (version << 24);
    
    //
    //  "packed schema" version
    //
    
    return version | 0x80;
  }
  
  /*****************************************
  *
  *  unpackSchemaVersion
  *
  *****************************************/

  public static int unpackSchemaVersion3(int packedVersion) { return (normalizeHistoricalPackedSchemaVersion(packedVersion) >> 24) & 0x7F; }
  public static int unpackSchemaVersion2(int packedVersion) { return (normalizeHistoricalPackedSchemaVersion(packedVersion) >> 16) & 0x7F; }
  public static int unpackSchemaVersion1(int packedVersion) { return (normalizeHistoricalPackedSchemaVersion(packedVersion) >>  8) & 0x7F; }
  public static int unpackSchemaVersion0(int packedVersion) { return (normalizeHistoricalPackedSchemaVersion(packedVersion) >>  0) & 0x7F; }

  //
  //  normalizeHistoricalPackedSchemaVersion
  //

  private static int normalizeHistoricalPackedSchemaVersion(int packedVersion)
  {
    int result = packedVersion;
    if (((packedVersion & 0x80) == 0) && ((packedVersion & 0x7F00) != 0))
      {
        int byte0 = (packedVersion >>  0) & 0x7F;
        int byte1 = (packedVersion >>  8) & 0x7F;
        int byte2 = (packedVersion >> 16) & 0x7F;
        int byte3 = (packedVersion >> 24) & 0x7F;
        result = (byte3 << 24) | (byte2 << 16) | (byte0 << 8) | (byte1 << 0);
      }
    return result;
  }
}
