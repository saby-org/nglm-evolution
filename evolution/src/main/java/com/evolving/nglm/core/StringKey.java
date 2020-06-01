/*****************************************************************************
*
*  StringKey.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

public class StringKey implements Comparable<StringKey>
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = Schema.STRING_SCHEMA;

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String key;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public StringKey(String key)
  {
    this.key = key;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getKey() { return key; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<StringKey> serde()
  {
    return new ConnectSerde<StringKey>(schema, true, StringKey.class, StringKey::pack, StringKey::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    return ((StringKey) value).getKey();
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static StringKey unpack(SchemaAndValue schemaAndValue)
  {
    return new StringKey((String) schemaAndValue.value());
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
    if (obj instanceof StringKey)
      {
        StringKey stringKey = (StringKey) obj;
        result = key.equals(stringKey.getKey());
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

  public int compareTo(StringKey other)
  {
    return key.compareTo(other.key);
  }
}
