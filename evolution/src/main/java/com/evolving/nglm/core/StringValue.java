/*****************************************************************************
*
*  StringValue.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

public class StringValue implements Comparable<StringValue>
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

  private String value;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public StringValue(String value)
  {
    this.value = value;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getValue() { return value; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<StringValue> serde()
  {
    return new ConnectSerde<StringValue>(schema, false, StringValue.class, StringValue::pack, StringValue::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    return ((StringValue) value).getValue();
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static StringValue unpack(SchemaAndValue schemaAndValue)
  {
    return new StringValue((String) schemaAndValue.value());
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
    if (obj instanceof StringValue)
      {
        StringValue stringValue = (StringValue) obj;
        result = value.equals(stringValue.getValue());
      }
    return result;
  }

  //
  //  hashCode
  //

  public int hashCode()
  {
    return value.hashCode();
  }

  /*****************************************
  *
  *  compareable
  *
  *****************************************/

  public int compareTo(StringValue other)
  {
    return value.compareTo(other.value);
  }
}
