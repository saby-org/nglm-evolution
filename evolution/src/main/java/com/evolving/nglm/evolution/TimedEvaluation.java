/*****************************************************************************
*
*  TimedEvaluation.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class TimedEvaluation implements SubscriberStreamEvent, SubscriberStreamOutput, Comparable
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("timed_evaluation");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private Date evaluationDate;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TimedEvaluation(String subscriberID, Date evaluationDate)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEvaluationDate() { return evaluationDate; }
  public Date getEventDate() { return evaluationDate; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<TimedEvaluation> serde()
  {
    return new ConnectSerde<TimedEvaluation>(schema, false, TimedEvaluation.class, TimedEvaluation::pack, TimedEvaluation::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    TimedEvaluation timedEvaluation = (TimedEvaluation) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", timedEvaluation.getSubscriberID());
    struct.put("evaluationDate", timedEvaluation.getEvaluationDate());
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static TimedEvaluation unpack(SchemaAndValue schemaAndValue)
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
    String subscriberID = valueStruct.getString("subscriberID");
    Date evaluationDate = (Date) valueStruct.get("evaluationDate");

    //
    //  return
    //

    return new TimedEvaluation(subscriberID, evaluationDate);
  }

  /*****************************************
  *
  *  equals (for SortedSet)
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof TimedEvaluation)
      {
        TimedEvaluation entry = (TimedEvaluation) obj;
        result = true;
        result = result && subscriberID.equals(entry.getSubscriberID());
        result = result && evaluationDate.equals(entry.getEvaluationDate());
      }
    return result;
  }

  /*****************************************
  *
  *  compareTo (for SortedSet)
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof TimedEvaluation)
      {
        TimedEvaluation entry = (TimedEvaluation) obj;
        result = evaluationDate.compareTo(entry.getEvaluationDate());
        if (result == 0) result = subscriberID.compareTo(entry.getSubscriberID());
      }
    return result;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    return "TimedEvaluation[" + subscriberID + "," + evaluationDate + "]";
  }
}
