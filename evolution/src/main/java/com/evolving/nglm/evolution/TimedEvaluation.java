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
import java.util.Objects;

public class TimedEvaluation implements EvolutionEngineEvent, Comparable
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
    schemaBuilder.field("periodicEvaluation", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("extendedSubscriberProfile", ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().optionalSchema());
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
  private boolean periodicEvaluation;
  private ExtendedSubscriberProfile extendedSubscriberProfile;

  /*****************************************
  *
  *  constructor (normal)
  *
  *****************************************/

  //
  //  TimedEvaluation
  //  

  public TimedEvaluation(String subscriberID, Date evaluationDate)
  {
    this(subscriberID, evaluationDate, false);
  }

  //
  //  PeriodicEvaluation
  //

  public TimedEvaluation(String subscriberID, Date evaluationDate, boolean periodicEvaluation)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
    this.periodicEvaluation = periodicEvaluation;
    this.extendedSubscriberProfile = null;
  }

  /*****************************************
  *
  *  construct (copy)
  *
  *****************************************/

  public TimedEvaluation(TimedEvaluation timedEvaluation)
  {
    this.subscriberID = timedEvaluation.getSubscriberID();
    this.evaluationDate = timedEvaluation.getEvaluationDate();
    this.periodicEvaluation = timedEvaluation.getPeriodicEvaluation();
    this.extendedSubscriberProfile = timedEvaluation.getExtendedSubscriberProfile();
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private TimedEvaluation(String subscriberID, Date evaluationDate, boolean periodicEvaluation, ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
    this.periodicEvaluation = periodicEvaluation;
    this.extendedSubscriberProfile = extendedSubscriberProfile;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEvaluationDate() { return evaluationDate; }
  public Date getEventDate() { return evaluationDate; }
  public boolean getPeriodicEvaluation() { return periodicEvaluation; }
  public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setExtendedSubscriberProfile(ExtendedSubscriberProfile extendedSubscriberProfile) { this.extendedSubscriberProfile = extendedSubscriberProfile; }

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
    struct.put("periodicEvaluation", timedEvaluation.getPeriodicEvaluation());
    struct.put("extendedSubscriberProfile", ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().packOptional(timedEvaluation.getExtendedSubscriberProfile()));
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
    boolean periodicEvaluation = (schemaVersion >= 2) ? valueStruct.getBoolean("periodicEvaluation") : false;
    ExtendedSubscriberProfile extendedSubscriberProfile = (schemaVersion >= 2) ? ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().unpackOptional(new SchemaAndValue(schema.field("extendedSubscriberProfile").schema(), valueStruct.get("extendedSubscriberProfile"))) : null;

    //
    //  return
    //

    return new TimedEvaluation(subscriberID, evaluationDate, periodicEvaluation, extendedSubscriberProfile);
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
        result = result && Objects.equals(subscriberID, entry.getSubscriberID());
        result = result && Objects.equals(evaluationDate, entry.getEvaluationDate());
        result = result && periodicEvaluation == entry.getPeriodicEvaluation();
        result = result && Objects.equals(extendedSubscriberProfile, entry.getExtendedSubscriberProfile());
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
    return "TimedEvaluation[" + subscriberID + "," + evaluationDate + (periodicEvaluation ? ", periodic" + (extendedSubscriberProfile != null ? ", extendedSubscriberProfile" : "") : "") + "]";
  }
  @Override
  public String getEventName()
  {
    if (periodicEvaluation == true) {
      return "dailyRoutine";
    }
    else {
      return "TimeOutEvent";
    }
  }
}
