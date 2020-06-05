/*****************************************************************************
*
*  DateValue.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class DateValue implements Comparable<DateValue>
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = Timestamp.SCHEMA;

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Date value;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DateValue(Date value)
  {
    this.value = value;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Date getValue() { return value; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<DateValue> serde()
  {
    return new ConnectSerde<DateValue>(schema, false, DateValue.class, DateValue::pack, DateValue::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    return ((DateValue) value).getValue();
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DateValue unpack(SchemaAndValue schemaAndValue)
  {
    return new DateValue((Date) schemaAndValue.value());
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
    if (obj instanceof DateValue)
      {
        DateValue dateValue = (DateValue) obj;
        result = value.equals(dateValue.getValue());
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

  public int compareTo(DateValue other)
  {
    return value.compareTo(other.value);
  }
}
