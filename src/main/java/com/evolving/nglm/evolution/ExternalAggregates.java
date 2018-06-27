/*****************************************************************************
*
*  ExternalAggregates.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.AutoProvisionSubscriberStreamEvent;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ExternalAggregates implements AutoProvisionSubscriberStreamEvent
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  SubscriberStatus
  //

  public enum SubscriberStatus
  {
    Active("A","active"),
    Inactive("S","inactive"),
    TypeC("C","typeC"),
    TypeR("R","typeR"),
    Unknown("(unknown)","(unknown)");
    private String externalRepresentation;
    private String stringRepresentation;
    private SubscriberStatus(String externalRepresentation, String stringRepresentation) { this.externalRepresentation = externalRepresentation; this.stringRepresentation = stringRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getStringRepresentation() { return stringRepresentation; }
    public static SubscriberStatus fromExternalRepresentation(String externalRepresentation) { for (SubscriberStatus enumeratedValue : SubscriberStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.name("external_aggregates");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("accountType", Schema.STRING_SCHEMA);
    schemaBuilder.field("primaryTariffPlan", Schema.STRING_SCHEMA);
    schemaBuilder.field("activationDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("daysInCurrentStatus", Schema.INT32_SCHEMA);
    schemaBuilder.field("previousSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("lastRechargeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("tariffPlanChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("mainBalanceValue", Schema.INT64_SCHEMA);
    schemaBuilder.field("totalCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("dataCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("callsCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("rechargeCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("rechargeCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("moCallsCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("moCallsCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("moCallsDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("mtCallsCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("mtCallsDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("mtCallsIntCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("mtCallsIntDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("moCallsIntCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("moCallsIntCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("moCallsIntDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("moSMSCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("moSMSCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("dataVolume", Schema.INT64_SCHEMA);
    schemaBuilder.field("dataBundleCharge", Schema.INT64_SCHEMA);
    schemaBuilder.field("subscriberRegion", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ExternalAggregates> serde = new ConnectSerde<ExternalAggregates>(schema, false, ExternalAggregates.class, ExternalAggregates::pack, ExternalAggregates::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ExternalAggregates> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private String accountType;
  private String primaryTariffPlan;
  private Date activationDate;
  private SubscriberStatus subscriberStatus;
  private int daysInCurrentStatus;
  private SubscriberStatus previousSubscriberStatus;
  private Date lastRechargeDate;
  private Date tariffPlanChangeDate;
  private long mainBalanceValue;
  private long totalCharge;
  private long dataCharge;
  private long callsCharge;
  private long rechargeCharge;
  private int rechargeCount;
  private long moCallsCharge;
  private int moCallsCount;
  private int moCallsDuration;
  private int mtCallsCount;
  private int mtCallsDuration;
  private int mtCallsIntCount;
  private int mtCallsIntDuration;
  private long moCallsIntCharge;
  private int moCallsIntCount;
  private int moCallsIntDuration;
  private long moSMSCharge;
  private int moSMSCount;
  private long dataVolume;
  private long dataBundleCharge;
  private String subscriberRegion;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getAccountType() { return accountType; }
  public String getPrimaryTariffPlan() { return primaryTariffPlan; }
  public Date getActivationDate() { return activationDate; }
  public SubscriberStatus getSubscriberStatus() { return subscriberStatus; }
  public int getDaysInCurrentStatus() { return daysInCurrentStatus; }
  public SubscriberStatus getPreviousSubscriberStatus() { return previousSubscriberStatus; }
  public Date getLastRechargeDate() { return lastRechargeDate; }
  public Date getTariffPlanChangeDate() { return tariffPlanChangeDate; }
  public long getMainBalanceValue() { return mainBalanceValue; }
  public long getTotalCharge() { return totalCharge; }
  public long getDataCharge() { return dataCharge; }
  public long getCallsCharge() { return callsCharge; }
  public long getRechargeCharge() { return rechargeCharge; }
  public int getRechargeCount() { return rechargeCount; }
  public long getMOCallsCharge() { return moCallsCharge; }
  public int getMOCallsCount() { return moCallsCount; }
  public int getMOCallsDuration() { return moCallsDuration; }
  public int getMTCallsCount() { return mtCallsCount; }
  public int getMTCallsDuration() { return mtCallsDuration; }
  public int getMTCallsIntCount() { return mtCallsIntCount; }
  public int getMTCallsIntDuration() { return mtCallsIntDuration; }
  public long getMOCallsIntCharge() { return moCallsIntCharge; }
  public int getMOCallsIntCount() { return moCallsIntCount; }
  public int getMOCallsIntDuration() { return moCallsIntDuration; }
  public long getMOSMSCharge() { return moSMSCharge; }
  public int getMOSMSCount() { return moSMSCount; }
  public long getDataVolume() { return dataVolume; }
  public long getDataBundleCharge() { return dataBundleCharge; }
  public String getSubscriberRegion() { return subscriberRegion; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  @Override public void rebindSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ExternalAggregates(String subscriberID, Date eventDate, String accountType, String primaryTariffPlan, Date activationDate, SubscriberStatus subscriberStatus, int daysInCurrentStatus, SubscriberStatus previousSubscriberStatus, Date lastRechargeDate, Date tariffPlanChangeDate, long mainBalanceValue, long totalCharge, long dataCharge, long callsCharge, long rechargeCharge, int rechargeCount, long moCallsCharge, int moCallsCount, int moCallsDuration, int mtCallsCount, int mtCallsDuration, int mtCallsIntCount, int mtCallsIntDuration, long moCallsIntCharge, int moCallsIntCount, int moCallsIntDuration, long moSMSCharge, int moSMSCount, long dataVolume, long dataBundleCharge, String subscriberRegion)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.accountType = accountType;
    this.primaryTariffPlan = primaryTariffPlan;
    this.activationDate = activationDate;
    this.subscriberStatus = subscriberStatus;
    this.daysInCurrentStatus = daysInCurrentStatus;
    this.previousSubscriberStatus = previousSubscriberStatus;
    this.lastRechargeDate = lastRechargeDate;
    this.tariffPlanChangeDate = tariffPlanChangeDate;
    this.mainBalanceValue = mainBalanceValue;
    this.totalCharge = totalCharge;
    this.dataCharge = dataCharge;
    this.callsCharge = callsCharge;
    this.rechargeCharge = rechargeCharge;
    this.rechargeCount = rechargeCount;
    this.moCallsCharge = moCallsCharge;
    this.moCallsCount = moCallsCount;
    this.moCallsDuration = moCallsDuration;
    this.mtCallsCount = mtCallsCount;
    this.mtCallsDuration = mtCallsDuration;
    this.mtCallsIntCount = mtCallsIntCount;
    this.mtCallsIntDuration = mtCallsIntDuration;
    this.moCallsIntCharge = moCallsIntCharge;
    this.moCallsIntCount = moCallsIntCount;
    this.moCallsIntDuration = moCallsIntDuration;
    this.moSMSCharge = moSMSCharge;
    this.moSMSCount = moSMSCount;
    this.dataVolume = dataVolume;
    this.dataBundleCharge = dataBundleCharge;
    this.subscriberRegion = subscriberRegion;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ExternalAggregates externalAggregates = (ExternalAggregates) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", externalAggregates.getSubscriberID());
    struct.put("eventDate", externalAggregates.getEventDate());
    struct.put("accountType", externalAggregates.getAccountType());
    struct.put("primaryTariffPlan", externalAggregates.getPrimaryTariffPlan());
    struct.put("activationDate", externalAggregates.getActivationDate());
    struct.put("subscriberStatus", externalAggregates.getSubscriberStatus().getExternalRepresentation());
    struct.put("daysInCurrentStatus", externalAggregates.getDaysInCurrentStatus());
    struct.put("previousSubscriberStatus", (externalAggregates.getPreviousSubscriberStatus() != null ? externalAggregates.getPreviousSubscriberStatus().getExternalRepresentation() : null));
    struct.put("lastRechargeDate", externalAggregates.getLastRechargeDate());
    struct.put("tariffPlanChangeDate", externalAggregates.getTariffPlanChangeDate());
    struct.put("mainBalanceValue", externalAggregates.getMainBalanceValue());
    struct.put("totalCharge", externalAggregates.getTotalCharge());
    struct.put("dataCharge", externalAggregates.getDataCharge());
    struct.put("callsCharge", externalAggregates.getCallsCharge());
    struct.put("rechargeCharge", externalAggregates.getRechargeCharge());
    struct.put("rechargeCount", externalAggregates.getRechargeCount());
    struct.put("moCallsCharge", externalAggregates.getMOCallsCharge());
    struct.put("moCallsCount", externalAggregates.getMOCallsCount());
    struct.put("moCallsDuration", externalAggregates.getMOCallsDuration());
    struct.put("mtCallsCount", externalAggregates.getMTCallsCount());
    struct.put("mtCallsDuration", externalAggregates.getMTCallsDuration());
    struct.put("mtCallsIntCount", externalAggregates.getMTCallsIntCount());
    struct.put("mtCallsIntDuration", externalAggregates.getMTCallsIntDuration());
    struct.put("moCallsIntCharge", externalAggregates.getMOCallsIntCharge());
    struct.put("moCallsIntCount", externalAggregates.getMOCallsIntCount());
    struct.put("moCallsIntDuration", externalAggregates.getMOCallsIntDuration());
    struct.put("moSMSCharge", externalAggregates.getMOSMSCharge());
    struct.put("moSMSCount", externalAggregates.getMOSMSCount());
    struct.put("dataVolume", externalAggregates.getDataVolume());
    struct.put("dataBundleCharge", externalAggregates.getDataBundleCharge());
    struct.put("subscriberRegion", externalAggregates.getSubscriberRegion());
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

  public static ExternalAggregates unpack(SchemaAndValue schemaAndValue)
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
    Date eventDate = (Date) valueStruct.get("eventDate");
    String accountType = valueStruct.getString("accountType");
    String primaryTariffPlan = valueStruct.getString("primaryTariffPlan");
    Date activationDate = (Date) valueStruct.get("activationDate");
    SubscriberStatus subscriberStatus = SubscriberStatus.fromExternalRepresentation(valueStruct.getString("subscriberStatus"));
    int daysInCurrentStatus = valueStruct.getInt32("daysInCurrentStatus");
    SubscriberStatus previousSubscriberStatus = (valueStruct.getString("previousSubscriberStatus") != null) ? SubscriberStatus.fromExternalRepresentation(valueStruct.getString("previousSubscriberStatus")) : null;
    Date lastRechargeDate = (Date) valueStruct.get("lastRechargeDate");
    Date tariffPlanChangeDate = (Date) valueStruct.get("tariffPlanChangeDate");
    long mainBalanceValue = valueStruct.getInt64("mainBalanceValue");
    long totalCharge = valueStruct.getInt64("totalCharge");
    long dataCharge = valueStruct.getInt64("dataCharge");
    long callsCharge = valueStruct.getInt64("callsCharge");
    long rechargeCharge = valueStruct.getInt64("rechargeCharge");
    int rechargeCount = valueStruct.getInt32("rechargeCount");
    long moCallsCharge = valueStruct.getInt64("moCallsCharge");
    int moCallsCount = valueStruct.getInt32("moCallsCount");
    int moCallsDuration = valueStruct.getInt32("moCallsDuration");
    int mtCallsCount = valueStruct.getInt32("mtCallsCount");
    int mtCallsDuration = valueStruct.getInt32("mtCallsDuration");
    int mtCallsIntCount = valueStruct.getInt32("mtCallsIntCount");
    int mtCallsIntDuration = valueStruct.getInt32("mtCallsIntDuration");
    long moCallsIntCharge = valueStruct.getInt64("moCallsIntCharge");
    int moCallsIntCount = valueStruct.getInt32("moCallsIntCount");
    int moCallsIntDuration = valueStruct.getInt32("moCallsIntDuration");
    long moSMSCharge = valueStruct.getInt64("moSMSCharge");
    int moSMSCount = valueStruct.getInt32("moSMSCount");
    long dataVolume = valueStruct.getInt64("dataVolume");
    long dataBundleCharge = valueStruct.getInt64("dataBundleCharge");
    String subscriberRegion = valueStruct.getString("subscriberRegion");
    
    //
    //  return
    //

    return new ExternalAggregates(subscriberID, eventDate, accountType, primaryTariffPlan, activationDate, subscriberStatus, daysInCurrentStatus, previousSubscriberStatus, lastRechargeDate, tariffPlanChangeDate, mainBalanceValue, totalCharge, dataCharge, callsCharge, rechargeCharge, rechargeCount, moCallsCharge, moCallsCount, moCallsDuration, mtCallsCount, mtCallsDuration, mtCallsIntCount, mtCallsIntDuration, moCallsIntCharge, moCallsIntCount, moCallsIntDuration, moSMSCharge, moSMSCount, dataVolume, dataBundleCharge, subscriberRegion);
  }
}