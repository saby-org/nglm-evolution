/*****************************************************************************
*
*  SubscriberProfile.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.ExternalAggregates.SubscriberStatus;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;

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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubscriberProfile implements SubscriberStreamOutput
{
  /*****************************************
  *
  *  static
  *
  *****************************************/

  public final static String ControlGroup = "controlgroup";
  public final static String UniversalControlGroup = "universalcontrolgroup";
  
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
    schemaBuilder.name("subscriber_profile");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberTraceEnabled", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("msisdn", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contractID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("accountTypeID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("ratePlan", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("activationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("subscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("statusChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("previousSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("lastRechargeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("ratePlanChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("mainBalanceValue", Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field("subscriberGroups", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("subscriber_profile_subscribergroups").schema());
    schemaBuilder.field("totalChargeHistory", MetricHistory.schema());
    schemaBuilder.field("dataChargeHistory", MetricHistory.schema());
    schemaBuilder.field("callsChargeHistory", MetricHistory.schema());
    schemaBuilder.field("rechargeChargeHistory", MetricHistory.schema());
    schemaBuilder.field("rechargeCountHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsChargeHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsCountHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsDurationHistory", MetricHistory.schema());
    schemaBuilder.field("mtCallsCountHistory", MetricHistory.schema());
    schemaBuilder.field("mtCallsDurationHistory", MetricHistory.schema());
    schemaBuilder.field("mtCallsIntCountHistory", MetricHistory.schema());
    schemaBuilder.field("mtCallsIntDurationHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsIntChargeHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsIntCountHistory", MetricHistory.schema());
    schemaBuilder.field("moCallsIntDurationHistory", MetricHistory.schema());
    schemaBuilder.field("moSMSChargeHistory", MetricHistory.schema());
    schemaBuilder.field("moSMSCountHistory", MetricHistory.schema());
    schemaBuilder.field("dataVolumeHistory", MetricHistory.schema());
    schemaBuilder.field("dataBundleChargeHistory", MetricHistory.schema());
    schemaBuilder.field("language", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("region", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberProfile> serde = new ConnectSerde<SubscriberProfile>(schema, false, SubscriberProfile.class, SubscriberProfile::pack, SubscriberProfile::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberProfile> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private boolean subscriberTraceEnabled;
  private Date activationDate;
  private String msisdn;
  private String contractID;
  private String accountTypeID;
  private String ratePlan;
  private SubscriberStatus subscriberStatus;
  private Date statusChangeDate;
  private SubscriberStatus previousSubscriberStatus;
  private Date lastRechargeDate;
  private Date ratePlanChangeDate;
  private Long mainBalanceValue;
  private Map<String, Integer> subscriberGroups;
  private MetricHistory totalChargeHistory;
  private MetricHistory dataChargeHistory;
  private MetricHistory callsChargeHistory;
  private MetricHistory rechargeChargeHistory;
  private MetricHistory rechargeCountHistory;
  private MetricHistory moCallsChargeHistory;
  private MetricHistory moCallsCountHistory;
  private MetricHistory moCallsDurationHistory;
  private MetricHistory mtCallsCountHistory;
  private MetricHistory mtCallsDurationHistory;
  private MetricHistory mtCallsIntCountHistory;
  private MetricHistory mtCallsIntDurationHistory;
  private MetricHistory moCallsIntChargeHistory;
  private MetricHistory moCallsIntCountHistory;
  private MetricHistory moCallsIntDurationHistory;
  private MetricHistory moSMSChargeHistory;
  private MetricHistory moSMSCountHistory;
  private MetricHistory dataVolumeHistory;
  private MetricHistory dataBundleChargeHistory;
  private String language;
  private String region;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public boolean getSubscriberTraceEnabled() { return subscriberTraceEnabled; }
  public String getMSISDN() { return msisdn; }
  public String getContractID() { return contractID; }
  public String getAccountTypeID() { return accountTypeID; }
  public String getRatePlan() { return ratePlan; }
  public Date getActivationDate() { return activationDate; }
  public SubscriberStatus getSubscriberStatus() { return subscriberStatus; }
  public Date getStatusChangeDate() { return statusChangeDate; }
  public SubscriberStatus getPreviousSubscriberStatus() { return previousSubscriberStatus; }
  public Date getLastRechargeDate() { return lastRechargeDate; }
  public Date getRatePlanChangeDate() { return ratePlanChangeDate; }
  public Long getMainBalanceValue() { return mainBalanceValue; }
  public Map<String, Integer> getSubscriberGroups() { return subscriberGroups; }
  public boolean getControlGroup(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) { return getInSubscriberGroup(ControlGroup, subscriberGroupEpochReader); }
  public boolean getUniversalControlGroup(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) { return getInSubscriberGroup(UniversalControlGroup, subscriberGroupEpochReader); }
  public MetricHistory getTotalChargeHistory() { return totalChargeHistory; }
  public MetricHistory getDataChargeHistory() { return dataChargeHistory; }
  public MetricHistory getCallsChargeHistory() { return callsChargeHistory; }
  public MetricHistory getRechargeChargeHistory() { return rechargeChargeHistory; }
  public MetricHistory getRechargeCountHistory() { return rechargeCountHistory; }
  public MetricHistory getMOCallsChargeHistory() { return moCallsChargeHistory; }
  public MetricHistory getMOCallsCountHistory() { return moCallsCountHistory; }
  public MetricHistory getMOCallsDurationHistory() { return moCallsDurationHistory; }
  public MetricHistory getMTCallsCountHistory() { return mtCallsCountHistory; }
  public MetricHistory getMTCallsDurationHistory() { return mtCallsDurationHistory; }
  public MetricHistory getMTCallsIntCountHistory() { return mtCallsIntCountHistory; }
  public MetricHistory getMTCallsIntDurationHistory() { return mtCallsIntDurationHistory; }
  public MetricHistory getMOCallsIntChargeHistory() { return moCallsIntChargeHistory; }
  public MetricHistory getMOCallsIntCountHistory() { return moCallsIntCountHistory; }
  public MetricHistory getMOCallsIntDurationHistory() { return moCallsIntDurationHistory; }
  public MetricHistory getMOSMSChargeHistory() { return moSMSChargeHistory; }
  public MetricHistory getMOSMSCountHistory() { return moSMSCountHistory; }
  public MetricHistory getDataVolumeHistory() { return dataVolumeHistory; }
  public MetricHistory getDataBundleChargeHistory() { return dataBundleChargeHistory; }
  public String getLanguage() { return language; }
  public String getRegion() { return region; }

  /****************************************
  *
  *  accessors - subscriberGroups
  *
  ****************************************/

  //
  //  getSubscriberGroups
  //

  public Set<String> getSubscriberGroups(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Set<String> result = new HashSet<String>();
    for (String groupName : subscriberGroups.keySet())
      {
        if (subscriberGroups.get(groupName) >= subscriberGroupEpochReader.get(groupName).getEpoch())
          {
            result.add(groupName);
          }
      }
    return result;
  }
  
  //
  //  getInSubscriberGroup
  //
  
  public boolean getInSubscriberGroup(String groupName, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    groupName = groupName.toLowerCase();
    int subscriberEpoch = (subscriberGroups.get(groupName) != null) ? subscriberGroups.get(groupName) : -1;
    int groupEpoch = (subscriberGroupEpochReader.get(groupName) != null) ? subscriberGroupEpochReader.get(groupName).getEpoch() : 0;
    return (subscriberEpoch >= groupEpoch);
  }
  
  /****************************************
  *
  *  accessors - history
  *
  ****************************************/

  //
  //  totalCharge
  //

  public long getHistoryTotalChargeYesterday(Date evaluationDate) { return getYesterday(totalChargeHistory, evaluationDate); }
  public long getHistoryTotalChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(totalChargeHistory, evaluationDate); }
  public long getHistoryTotalChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(totalChargeHistory, evaluationDate); }
  public long getHistoryTotalChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(totalChargeHistory, evaluationDate); }

  //
  //  rechargeCount
  //

  public long getHistoryRechargeCountYesterday(Date evaluationDate) { return getYesterday(rechargeCountHistory, evaluationDate); }
  public long getHistoryRechargeCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(rechargeCountHistory, evaluationDate); }
  public long getHistoryRechargeCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(rechargeCountHistory, evaluationDate); }
  public long getHistoryRechargeCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(rechargeCountHistory, evaluationDate); }

  //
  //  rechargeCharge
  //

  public long getHistoryRechargeChargeYesterday(Date evaluationDate) { return getYesterday(rechargeChargeHistory, evaluationDate); }
  public long getHistoryRechargeChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(rechargeChargeHistory, evaluationDate); }
  public long getHistoryRechargeChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(rechargeChargeHistory, evaluationDate); }
  public long getHistoryRechargeChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(rechargeChargeHistory, evaluationDate); }

  //
  //  moCallCharge
  //

  public long getHistoryMOCallChargeYesterday(Date evaluationDate) { return getYesterday(moCallsChargeHistory, evaluationDate); }
  public long getHistoryMOCallChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsChargeHistory, evaluationDate); }
  public long getHistoryMOCallChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsChargeHistory, evaluationDate); }
  public long getHistoryMOCallChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsChargeHistory, evaluationDate); }

  //
  //  moCallCount
  //

  public long getHistoryMOCallCountYesterday(Date evaluationDate) { return getYesterday(moCallsCountHistory, evaluationDate); }
  public long getHistoryMOCallCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsCountHistory, evaluationDate); }
  public long getHistoryMOCallCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsCountHistory, evaluationDate); }
  public long getHistoryMOCallCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsCountHistory, evaluationDate); }

  //
  //  moCallDuration
  //

  public long getHistoryMOCallDurationYesterday(Date evaluationDate) { return getYesterday(moCallsDurationHistory, evaluationDate); }
  public long getHistoryMOCallDurationPrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsDurationHistory, evaluationDate); }
  public long getHistoryMOCallDurationPrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsDurationHistory, evaluationDate); }
  public long getHistoryMOCallDurationPreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsDurationHistory, evaluationDate); }

  //
  //  mtCallCount
  //

  public long getHistoryMTCallCountYesterday(Date evaluationDate) { return getYesterday(mtCallsCountHistory, evaluationDate); }
  public long getHistoryMTCallCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(mtCallsCountHistory, evaluationDate); }
  public long getHistoryMTCallCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(mtCallsCountHistory, evaluationDate); }
  public long getHistoryMTCallCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(mtCallsCountHistory, evaluationDate); }

  //
  //  mtCallDuration
  //

  public long getHistoryMTCallDurationYesterday(Date evaluationDate) { return getYesterday(mtCallsDurationHistory, evaluationDate); }
  public long getHistoryMTCallDurationPrevious7Days(Date evaluationDate) { return getPrevious7Days(mtCallsDurationHistory, evaluationDate); }
  public long getHistoryMTCallDurationPrevious14Days(Date evaluationDate) { return getPrevious14Days(mtCallsDurationHistory, evaluationDate); }
  public long getHistoryMTCallDurationPreviousMonth(Date evaluationDate) { return getPreviousMonth(mtCallsDurationHistory, evaluationDate); }

  //
  //  mtCallIntCount
  //

  public long getHistoryMTCallIntCountYesterday(Date evaluationDate) { return getYesterday(mtCallsIntCountHistory, evaluationDate); }
  public long getHistoryMTCallIntCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(mtCallsIntCountHistory, evaluationDate); }
  public long getHistoryMTCallIntCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(mtCallsIntCountHistory, evaluationDate); }
  public long getHistoryMTCallIntCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(mtCallsIntCountHistory, evaluationDate); }

  //
  //  mtCallIntDuration
  //

  public long getHistoryMTCallIntDurationYesterday(Date evaluationDate) { return getYesterday(mtCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMTCallIntDurationPrevious7Days(Date evaluationDate) { return getPrevious7Days(mtCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMTCallIntDurationPrevious14Days(Date evaluationDate) { return getPrevious14Days(mtCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMTCallIntDurationPreviousMonth(Date evaluationDate) { return getPreviousMonth(mtCallsIntDurationHistory, evaluationDate); }

  //
  //  moCallIntCharge
  //

  public long getHistoryMOCallIntChargeYesterday(Date evaluationDate) { return getYesterday(moCallsIntChargeHistory, evaluationDate); }
  public long getHistoryMOCallIntChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsIntChargeHistory, evaluationDate); }
  public long getHistoryMOCallIntChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsIntChargeHistory, evaluationDate); }
  public long getHistoryMOCallIntChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsIntChargeHistory, evaluationDate); }

  //
  //  moCallIntCount
  //

  public long getHistoryMOCallIntCountYesterday(Date evaluationDate) { return getYesterday(moCallsIntCountHistory, evaluationDate); }
  public long getHistoryMOCallIntCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsIntCountHistory, evaluationDate); }
  public long getHistoryMOCallIntCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsIntCountHistory, evaluationDate); }
  public long getHistoryMOCallIntCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsIntCountHistory, evaluationDate); }

  //
  //  moCallIntDuration
  //

  public long getHistoryMOCallIntDurationYesterday(Date evaluationDate) { return getYesterday(moCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMOCallIntDurationPrevious7Days(Date evaluationDate) { return getPrevious7Days(moCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMOCallIntDurationPrevious14Days(Date evaluationDate) { return getPrevious14Days(moCallsIntDurationHistory, evaluationDate); }
  public long getHistoryMOCallIntDurationPreviousMonth(Date evaluationDate) { return getPreviousMonth(moCallsIntDurationHistory, evaluationDate); }

  //
  //  moSMSCharge
  //

  public long getHistoryMOSMSChargeYesterday(Date evaluationDate) { return getYesterday(moSMSChargeHistory, evaluationDate); }
  public long getHistoryMOSMSChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(moSMSChargeHistory, evaluationDate); }
  public long getHistoryMOSMSChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(moSMSChargeHistory, evaluationDate); }
  public long getHistoryMOSMSChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(moSMSChargeHistory, evaluationDate); }

  //
  //  moSMSCount
  //

  public long getHistoryMOSMSCountYesterday(Date evaluationDate) { return getYesterday(moSMSCountHistory, evaluationDate); }
  public long getHistoryMOSMSCountPrevious7Days(Date evaluationDate) { return getPrevious7Days(moSMSCountHistory, evaluationDate); }
  public long getHistoryMOSMSCountPrevious14Days(Date evaluationDate) { return getPrevious14Days(moSMSCountHistory, evaluationDate); }
  public long getHistoryMOSMSCountPreviousMonth(Date evaluationDate) { return getPreviousMonth(moSMSCountHistory, evaluationDate); }

  //
  //  dataVolume
  //

  public long getHistoryDataVolumeYesterday(Date evaluationDate) { return getYesterday(dataVolumeHistory, evaluationDate); }
  public long getHistoryDataVolumePrevious7Days(Date evaluationDate) { return getPrevious7Days(dataVolumeHistory, evaluationDate); }
  public long getHistoryDataVolumePrevious14Days(Date evaluationDate) { return getPrevious14Days(dataVolumeHistory, evaluationDate); }
  public long getHistoryDataVolumePreviousMonth(Date evaluationDate) { return getPreviousMonth(dataVolumeHistory, evaluationDate); }

  //
  //  dataBundleCharge
  //

  public long getHistoryDataBundleChargeYesterday(Date evaluationDate) { return getYesterday(dataBundleChargeHistory, evaluationDate); }
  public long getHistoryDataBundleChargePrevious7Days(Date evaluationDate) { return getPrevious7Days(dataBundleChargeHistory, evaluationDate); }
  public long getHistoryDataBundleChargePrevious14Days(Date evaluationDate) { return getPrevious14Days(dataBundleChargeHistory, evaluationDate); }
  public long getHistoryDataBundleChargePreviousMonth(Date evaluationDate) { return getPreviousMonth(dataBundleChargeHistory, evaluationDate); }

  /****************************************
  *
  *  getHistory utilities
  *
  ****************************************/

  //
  //  getYesterday
  //
  
  public long getYesterday(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    Date endDay = startDay;
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPrevious7Days
  //
  
  public long getPrevious7Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -7, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPrevious14Days
  //
  
  public long getPrevious14Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -14, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPreviousMonth
  //
  
  public long getPreviousMonth(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startOfMonth = RLMDateUtils.truncate(day, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addMonths(startOfMonth, -1, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(startOfMonth, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setSubscriberTraceEnabled(boolean subscriberTraceEnabled) { this.subscriberTraceEnabled = subscriberTraceEnabled; }
  public void setMSISDN(String msisdn) { this.msisdn = msisdn; }
  public void setContractID(String contractID) { this.contractID = contractID; }
  public void setAccountTypeID(String accountTypeID) { this.accountTypeID = accountTypeID; }
  public void setRatePlan(String ratePlan) { this.ratePlan = ratePlan; }
  public void setActivationDate(Date activationDate) { this.activationDate = activationDate; }
  public void setSubscriberStatus(SubscriberStatus subscriberStatus) { this.subscriberStatus = subscriberStatus; }
  public void setStatusChangeDate(Date statusChangeDate) { this.statusChangeDate = statusChangeDate; }
  public void setPreviousSubscriberStatus(SubscriberStatus previousSubscriberStatus) { this.previousSubscriberStatus = previousSubscriberStatus; }
  public void setLastRechargeDate(Date lastRechargeDate) { this.lastRechargeDate = lastRechargeDate; }
  public void setRatePlanChangeDate(Date ratePlanChangeDate) { this.ratePlanChangeDate = ratePlanChangeDate; }
  public void setMainBalanceValue(Long mainBalanceValue) { this.mainBalanceValue = mainBalanceValue; }
  public void setLanguage(String language) { this.language = language; }
  public void setRegion(String region) { this.region = region; }

  //
  //  setSubscriberGroup
  //
  
  public void setSubscriberGroup(String groupName, int epoch, boolean addSubscriber)
  {
    groupName = (groupName != null) ? groupName.toLowerCase() : null;
    Integer existingEpoch = subscriberGroups.get(groupName);
    if (existingEpoch == null || epoch >= existingEpoch.intValue())
      {
        if (addSubscriber)
          subscriberGroups.put(groupName, epoch);
        else
          subscriberGroups.remove(groupName);
      }
  }
  
  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public SubscriberProfile(String subscriberID)
  {
    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = false;
    this.msisdn = null;
    this.contractID = null;
    this.accountTypeID = null;
    this.ratePlan = null;
    this.activationDate = null;
    this.subscriberStatus = null;
    this.statusChangeDate = null;
    this.previousSubscriberStatus = null;
    this.lastRechargeDate = null;
    this.ratePlanChangeDate = null;
    this.mainBalanceValue = null;
    this.subscriberGroups = new HashMap<String,Integer>();
    this.totalChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.dataChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.callsChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.rechargeChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.rechargeCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsDurationHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.mtCallsCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.mtCallsDurationHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.mtCallsIntCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.mtCallsIntDurationHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsIntChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsIntCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moCallsIntDurationHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moSMSChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.moSMSCountHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.dataVolumeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.dataBundleChargeHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS);
    this.language = null;
    this.region = null;
  }
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private SubscriberProfile(String subscriberID, boolean subscriberTraceEnabled, String msisdn, String contractID, String accountTypeID, String ratePlan, Date activationDate, SubscriberStatus subscriberStatus, Date statusChangeDate, SubscriberStatus previousSubscriberStatus, Date lastRechargeDate, Date ratePlanChangeDate, Long mainBalanceValue, Map<String,Integer> subscriberGroups, MetricHistory totalChargeHistory, MetricHistory dataChargeHistory, MetricHistory callsChargeHistory, MetricHistory rechargeChargeHistory, MetricHistory rechargeCountHistory, MetricHistory moCallsChargeHistory, MetricHistory moCallsCountHistory, MetricHistory moCallsDurationHistory, MetricHistory mtCallsCountHistory, MetricHistory mtCallsDurationHistory, MetricHistory mtCallsIntCountHistory, MetricHistory mtCallsIntDurationHistory, MetricHistory moCallsIntChargeHistory, MetricHistory moCallsIntCountHistory, MetricHistory moCallsIntDurationHistory, MetricHistory moSMSChargeHistory, MetricHistory moSMSCountHistory, MetricHistory dataVolumeHistory, MetricHistory dataBundleChargeHistory, String language, String region)
  {
    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = subscriberTraceEnabled;
    this.msisdn = msisdn;
    this.contractID = contractID;
    this.accountTypeID = accountTypeID;
    this.ratePlan = ratePlan;
    this.activationDate = activationDate;
    this.subscriberStatus = subscriberStatus;
    this.statusChangeDate = statusChangeDate;
    this.previousSubscriberStatus = previousSubscriberStatus;
    this.lastRechargeDate = lastRechargeDate;
    this.ratePlanChangeDate = ratePlanChangeDate;
    this.mainBalanceValue = mainBalanceValue;
    this.subscriberGroups = subscriberGroups;
    this.totalChargeHistory = totalChargeHistory;
    this.dataChargeHistory = dataChargeHistory;
    this.callsChargeHistory = callsChargeHistory;
    this.rechargeChargeHistory = rechargeChargeHistory;
    this.rechargeCountHistory = rechargeCountHistory;
    this.moCallsChargeHistory = moCallsChargeHistory;
    this.moCallsCountHistory = moCallsCountHistory;
    this.moCallsDurationHistory = moCallsDurationHistory;
    this.mtCallsCountHistory = mtCallsCountHistory;
    this.mtCallsDurationHistory = mtCallsDurationHistory;
    this.mtCallsIntCountHistory = mtCallsIntCountHistory;
    this.mtCallsIntDurationHistory = mtCallsIntDurationHistory;
    this.moCallsIntChargeHistory = moCallsIntChargeHistory;
    this.moCallsIntCountHistory = moCallsIntCountHistory;
    this.moCallsIntDurationHistory = moCallsIntDurationHistory;
    this.moSMSChargeHistory = moSMSChargeHistory;
    this.moSMSCountHistory = moSMSCountHistory;
    this.dataVolumeHistory = dataVolumeHistory;
    this.dataBundleChargeHistory = dataBundleChargeHistory;
    this.language = language;
    this.region = region;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public SubscriberProfile(SubscriberProfile subscriberProfile)
  {
    this.subscriberID = subscriberProfile.getSubscriberID();
    this.subscriberTraceEnabled = subscriberProfile.getSubscriberTraceEnabled();
    this.msisdn = subscriberProfile.getMSISDN();
    this.contractID = subscriberProfile.getContractID();
    this.accountTypeID = subscriberProfile.getAccountTypeID();
    this.ratePlan = subscriberProfile.getRatePlan();
    this.activationDate = subscriberProfile.getActivationDate();
    this.subscriberStatus = subscriberProfile.getSubscriberStatus();
    this.statusChangeDate = subscriberProfile.getStatusChangeDate();
    this.previousSubscriberStatus = subscriberProfile.getPreviousSubscriberStatus();
    this.lastRechargeDate = subscriberProfile.getLastRechargeDate();
    this.ratePlanChangeDate = subscriberProfile.getRatePlanChangeDate();
    this.mainBalanceValue = subscriberProfile.getMainBalanceValue();
    this.subscriberGroups = new HashMap<String,Integer>(subscriberProfile.getSubscriberGroups());
    this.totalChargeHistory = new MetricHistory(subscriberProfile.getTotalChargeHistory());
    this.dataChargeHistory = new MetricHistory(subscriberProfile.getDataChargeHistory());
    this.callsChargeHistory = new MetricHistory(subscriberProfile.getCallsChargeHistory());
    this.rechargeChargeHistory = new MetricHistory(subscriberProfile.getRechargeChargeHistory());
    this.rechargeCountHistory = new MetricHistory(subscriberProfile.getRechargeCountHistory());
    this.moCallsChargeHistory = new MetricHistory(subscriberProfile.getMOCallsChargeHistory());
    this.moCallsCountHistory = new MetricHistory(subscriberProfile.getMOCallsCountHistory());
    this.moCallsDurationHistory = new MetricHistory(subscriberProfile.getMOCallsDurationHistory());
    this.mtCallsCountHistory = new MetricHistory(subscriberProfile.getMTCallsCountHistory());
    this.mtCallsDurationHistory = new MetricHistory(subscriberProfile.getMTCallsDurationHistory());
    this.mtCallsIntCountHistory = new MetricHistory(subscriberProfile.getMTCallsIntCountHistory());
    this.mtCallsIntDurationHistory = new MetricHistory(subscriberProfile.getMTCallsIntDurationHistory());
    this.moCallsIntChargeHistory = new MetricHistory(subscriberProfile.getMOCallsIntChargeHistory());
    this.moCallsIntCountHistory = new MetricHistory(subscriberProfile.getMOCallsIntCountHistory());
    this.moCallsIntDurationHistory = new MetricHistory(subscriberProfile.getMOCallsIntDurationHistory());
    this.moSMSChargeHistory = new MetricHistory(subscriberProfile.getMOSMSChargeHistory());
    this.moSMSCountHistory = new MetricHistory(subscriberProfile.getMOSMSCountHistory());
    this.dataVolumeHistory = new MetricHistory(subscriberProfile.getDataVolumeHistory());
    this.dataBundleChargeHistory = new MetricHistory(subscriberProfile.getDataBundleChargeHistory());
    this.language = subscriberProfile.getLanguage();
    this.region = subscriberProfile.getRegion();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberProfile subscriberProfile = (SubscriberProfile) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberProfile.getSubscriberID());
    struct.put("subscriberTraceEnabled", subscriberProfile.getSubscriberTraceEnabled());
    struct.put("msisdn", subscriberProfile.getMSISDN());
    struct.put("contractID", subscriberProfile.getContractID());
    struct.put("accountTypeID", subscriberProfile.getAccountTypeID());
    struct.put("ratePlan", subscriberProfile.getRatePlan());
    struct.put("activationDate", subscriberProfile.getActivationDate());
    struct.put("subscriberStatus", (subscriberProfile.getSubscriberStatus() != null) ? subscriberProfile.getSubscriberStatus().getExternalRepresentation() : null);
    struct.put("statusChangeDate", subscriberProfile.getStatusChangeDate());
    struct.put("previousSubscriberStatus", (subscriberProfile.getPreviousSubscriberStatus() != null) ? subscriberProfile.getPreviousSubscriberStatus().getExternalRepresentation() : null);
    struct.put("lastRechargeDate", subscriberProfile.getLastRechargeDate());
    struct.put("ratePlanChangeDate", subscriberProfile.getRatePlanChangeDate());
    struct.put("mainBalanceValue", subscriberProfile.getMainBalanceValue());
    struct.put("subscriberGroups", subscriberProfile.getSubscriberGroups());
    struct.put("totalChargeHistory", MetricHistory.pack(subscriberProfile.getTotalChargeHistory()));
    struct.put("dataChargeHistory", MetricHistory.pack(subscriberProfile.getDataChargeHistory()));
    struct.put("callsChargeHistory", MetricHistory.pack(subscriberProfile.getCallsChargeHistory()));
    struct.put("rechargeChargeHistory", MetricHistory.pack(subscriberProfile.getRechargeChargeHistory()));
    struct.put("rechargeCountHistory", MetricHistory.pack(subscriberProfile.getRechargeCountHistory()));
    struct.put("moCallsChargeHistory", MetricHistory.pack(subscriberProfile.getMOCallsChargeHistory()));
    struct.put("moCallsCountHistory", MetricHistory.pack(subscriberProfile.getMOCallsCountHistory()));
    struct.put("moCallsDurationHistory", MetricHistory.pack(subscriberProfile.getMOCallsDurationHistory()));
    struct.put("mtCallsCountHistory", MetricHistory.pack(subscriberProfile.getMTCallsCountHistory()));
    struct.put("mtCallsDurationHistory", MetricHistory.pack(subscriberProfile.getMTCallsDurationHistory()));
    struct.put("mtCallsIntCountHistory", MetricHistory.pack(subscriberProfile.getMTCallsIntCountHistory()));
    struct.put("mtCallsIntDurationHistory", MetricHistory.pack(subscriberProfile.getMTCallsIntDurationHistory()));
    struct.put("moCallsIntChargeHistory", MetricHistory.pack(subscriberProfile.getMOCallsIntChargeHistory()));
    struct.put("moCallsIntCountHistory", MetricHistory.pack(subscriberProfile.getMOCallsIntCountHistory()));
    struct.put("moCallsIntDurationHistory", MetricHistory.pack(subscriberProfile.getMOCallsIntDurationHistory()));
    struct.put("moSMSChargeHistory", MetricHistory.pack(subscriberProfile.getMOSMSChargeHistory()));
    struct.put("moSMSCountHistory", MetricHistory.pack(subscriberProfile.getMOSMSCountHistory()));
    struct.put("dataVolumeHistory", MetricHistory.pack(subscriberProfile.getDataVolumeHistory()));
    struct.put("dataBundleChargeHistory", MetricHistory.pack(subscriberProfile.getDataBundleChargeHistory()));
    struct.put("language", subscriberProfile.getLanguage());
    struct.put("region", subscriberProfile.getRegion());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubscriberProfile unpack(SchemaAndValue schemaAndValue)
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
    Boolean subscriberTraceEnabled = valueStruct.getBoolean("subscriberTraceEnabled");
    String msisdn = valueStruct.getString("msisdn");
    String contractID = valueStruct.getString("contractID");
    String accountTypeID = valueStruct.getString("accountTypeID");
    String ratePlan = valueStruct.getString("ratePlan");
    Date activationDate = (Date) valueStruct.get("activationDate");
    SubscriberStatus subscriberStatus = (valueStruct.getString("subscriberStatus") != null) ? SubscriberStatus.fromExternalRepresentation(valueStruct.getString("subscriberStatus")) : null;
    Date statusChangeDate = (Date) valueStruct.get("statusChangeDate");
    SubscriberStatus previousSubscriberStatus = (valueStruct.getString("previousSubscriberStatus") != null) ? SubscriberStatus.fromExternalRepresentation(valueStruct.getString("previousSubscriberStatus")) : null;
    Date lastRechargeDate = (Date) valueStruct.get("lastRechargeDate");
    Date ratePlanChangeDate = (Date) valueStruct.get("ratePlanChangeDate");
    Long mainBalanceValue = valueStruct.getInt64("mainBalanceValue");
    Map<String,Integer> subscriberGroups = (Map<String,Integer>) valueStruct.get("subscriberGroups");
    MetricHistory totalChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("totalChargeHistory").schema(), valueStruct.get("totalChargeHistory")));
    MetricHistory dataChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("dataChargeHistory").schema(), valueStruct.get("dataChargeHistory")));
    MetricHistory callsChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("callsChargeHistory").schema(), valueStruct.get("callsChargeHistory")));
    MetricHistory rechargeChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("rechargeChargeHistory").schema(), valueStruct.get("rechargeChargeHistory")));
    MetricHistory rechargeCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("rechargeCountHistory").schema(), valueStruct.get("rechargeCountHistory")));
    MetricHistory moCallsChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsChargeHistory").schema(), valueStruct.get("moCallsChargeHistory")));
    MetricHistory moCallsCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsCountHistory").schema(), valueStruct.get("moCallsCountHistory")));
    MetricHistory moCallsDurationHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsDurationHistory").schema(), valueStruct.get("moCallsDurationHistory")));
    MetricHistory mtCallsCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("mtCallsCountHistory").schema(), valueStruct.get("mtCallsCountHistory")));
    MetricHistory mtCallsDurationHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("mtCallsDurationHistory").schema(), valueStruct.get("mtCallsDurationHistory")));
    MetricHistory mtCallsIntCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("mtCallsIntCountHistory").schema(), valueStruct.get("mtCallsIntCountHistory")));
    MetricHistory mtCallsIntDurationHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("mtCallsIntDurationHistory").schema(), valueStruct.get("mtCallsIntDurationHistory")));
    MetricHistory moCallsIntChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsIntChargeHistory").schema(), valueStruct.get("moCallsIntChargeHistory")));
    MetricHistory moCallsIntCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsIntCountHistory").schema(), valueStruct.get("moCallsIntCountHistory")));
    MetricHistory moCallsIntDurationHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moCallsIntDurationHistory").schema(), valueStruct.get("moCallsIntDurationHistory")));
    MetricHistory moSMSChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moSMSChargeHistory").schema(), valueStruct.get("moSMSChargeHistory")));
    MetricHistory moSMSCountHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("moSMSCountHistory").schema(), valueStruct.get("moSMSCountHistory")));
    MetricHistory dataVolumeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("dataVolumeHistory").schema(), valueStruct.get("dataVolumeHistory")));
    MetricHistory dataBundleChargeHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("dataBundleChargeHistory").schema(), valueStruct.get("dataBundleChargeHistory")));
    String language = valueStruct.getString("language");
    String region = valueStruct.getString("region");

    //
    //  return
    //

    return new SubscriberProfile(subscriberID, subscriberTraceEnabled, msisdn, contractID, accountTypeID, ratePlan, activationDate, subscriberStatus, statusChangeDate, previousSubscriberStatus, lastRechargeDate, ratePlanChangeDate, mainBalanceValue, subscriberGroups, totalChargeHistory, dataChargeHistory, callsChargeHistory, rechargeChargeHistory, rechargeCountHistory, moCallsChargeHistory, moCallsCountHistory, moCallsDurationHistory, mtCallsCountHistory, mtCallsDurationHistory, mtCallsIntCountHistory, mtCallsIntDurationHistory, moCallsIntChargeHistory, moCallsIntCountHistory, moCallsIntDurationHistory, moSMSChargeHistory, moSMSCountHistory, dataVolumeHistory, dataBundleChargeHistory, language, region);
  }
}
