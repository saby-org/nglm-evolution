/*****************************************************************************
*
*  LongKey.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

public class LongKey implements Comparable<LongKey>
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = Schema.INT64_SCHEMA;

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Long key;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public LongKey(Long key)
  {
    this.key = key;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Long getKey() { return key; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<LongKey> serde()
  {
    return new ConnectSerde<LongKey>(schema, true, LongKey.class, LongKey::pack, LongKey::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    return ((LongKey) value).getKey();
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LongKey unpack(SchemaAndValue schemaAndValue)
  {
    return new LongKey((Long) schemaAndValue.value());
  }

  /*****************************************
  *
  *  equals/hashCode
  *
  *****************************************/

  //
  //  equals
  //

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof LongKey)
      {
        LongKey longKey = (LongKey) obj;
        result = key.equals(longKey.getKey());
      }
    return result;
  }

  //
  //  hashCode
  //

  public int hashCode()
  {
    return key.hashCode();
  }

  /*****************************************
  *
  *  compareable
  *
  *****************************************/

  public int compareTo(LongKey other)
  {
    return key.compareTo(other.key);
  }
}
