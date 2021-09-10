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
import com.evolving.nglm.core.SystemTime;

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(4));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
    schemaBuilder.field("periodicEvaluation", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("targetProvisioning", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("extendedSubscriberProfile", ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().optionalSchema());
    schemaBuilder.field("infoSource", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("infoDuration", Schema.OPTIONAL_STRING_SCHEMA);
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
  private boolean targetProvisioning;
  private ExtendedSubscriberProfile extendedSubscriberProfile;
  private String infoSource;
  private String infoDuration;

  /*****************************************
  *
  *  constructor (normal)
  *
  *****************************************/

  //
  //  TimedEvaluation
  //  

  public TimedEvaluation(String subscriberID, Date evaluationDate, String infoSource)
  {
    this(subscriberID, evaluationDate, false, false, infoSource);
  }

  //
  //  PeriodicEvaluation
  //

  public TimedEvaluation(String subscriberID, Date evaluationDate, boolean periodicEvaluation, boolean targetProvisioning, String infoSource)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
    this.periodicEvaluation = periodicEvaluation;
    this.targetProvisioning = targetProvisioning;
    this.extendedSubscriberProfile = null;
    this.infoSource = infoSource;
    this.infoDuration = "" + ((evaluationDate.getTime() - SystemTime.getCurrentTime().getTime()) / 1000);
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
    this.targetProvisioning = timedEvaluation.getTargetProvisioning();
    this.extendedSubscriberProfile = timedEvaluation.getExtendedSubscriberProfile();
    this.infoSource = timedEvaluation.getInfoSource();
    this.infoDuration = timedEvaluation.getInfoDuration();
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private TimedEvaluation(String subscriberID, Date evaluationDate, boolean periodicEvaluation, boolean targetProvisioning, ExtendedSubscriberProfile extendedSubscriberProfile, String infoSource, String infoDuration)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
    this.periodicEvaluation = periodicEvaluation;
    this.targetProvisioning = targetProvisioning;
    this.extendedSubscriberProfile = extendedSubscriberProfile;
    this.infoSource = infoSource;
    this.infoDuration = infoDuration;
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
  public boolean getTargetProvisioning() { return targetProvisioning; }
  public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }
  public String getInfoSource() { return infoSource; }
  public String getInfoDuration() { return infoDuration; }

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.Low; }

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
    struct.put("targetProvisioning", timedEvaluation.getTargetProvisioning());
    struct.put("extendedSubscriberProfile", ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().packOptional(timedEvaluation.getExtendedSubscriberProfile()));
    struct.put("infoSource", timedEvaluation.getInfoSource());
    struct.put("infoDuration", timedEvaluation.getInfoDuration());
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
    boolean targetProvisioning = (schemaVersion >= 3 && schema.field("targetProvisioning")!=null) ? valueStruct.getBoolean("targetProvisioning") : false;
    ExtendedSubscriberProfile extendedSubscriberProfile = (schemaVersion >= 2) ? ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().unpackOptional(new SchemaAndValue(schema.field("extendedSubscriberProfile").schema(), valueStruct.get("extendedSubscriberProfile"))) : null;
    String infoSource = schema.field("infoSource") != null ? valueStruct.getString("infoSource") : null;
    String infoDuration = schema.field("infoDuration") != null ? valueStruct.getString("infoDuration") : null;
    //
    //  return
    //

    return new TimedEvaluation(subscriberID, evaluationDate, periodicEvaluation, targetProvisioning, extendedSubscriberProfile, infoSource, infoDuration);
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
        result = result && targetProvisioning == entry.getTargetProvisioning();
        result = result && Objects.equals(extendedSubscriberProfile, entry.getExtendedSubscriberProfile());
        // not use infoSource and infoDuration for comparison
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
    return "TimedEvaluation[" + subscriberID + "," + evaluationDate + (periodicEvaluation ? ", periodic" + (extendedSubscriberProfile != null ? ", extendedSubscriberProfile" : "") : "") + ", infoSource " + infoSource + ", infoDuration " + infoDuration + "]";
  }
  @Override
  public String getEventName()
  {
    if (periodicEvaluation) return "dailyRoutine";
    if (targetProvisioning) return "targetProvisioning";
    return "TimeOutEvent";
  }
}
