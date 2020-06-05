/*****************************************************************************
*
*  ReferenceDataContainsKey.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public class ReferenceDataContainsKey implements ReferenceDataValue<String>
{
  /****************************************
  *
  *  data
  *
  ****************************************/

  private String key;
  private boolean deleted;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getKey() { return key; }
  public boolean getDeleted() { return deleted; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ReferenceDataContainsKey(String key, boolean deleted)
  {
    this.key = key;
    this.deleted = deleted;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ReferenceDataContainsKey unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String key = valueStruct.getString("data_key");
    boolean deleted = valueStruct.getInt8("deleted").intValue() != 0;
    
    //
    //  return
    //

     return new ReferenceDataContainsKey(key, deleted);
  }
}
