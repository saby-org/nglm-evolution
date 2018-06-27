/****************************************************************************
*
*  SubscriberProfileESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.ReferenceDataReader;

import com.rii.utilities.SystemTime;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SubscriberProfileESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return SubscriberProfileESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class SubscriberProfileESSinkTask extends ChangeLogESSinkTask
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private String sinkTaskKey = Integer.toHexString(getTaskNumber()) + "-" + Integer.toHexString(ThreadLocalRandom.current().nextInt(1000000000));

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public SubscriberProfileESSinkTask()
    {
      super();
      this.subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("profileSinkConnector-subscriberGroupEpoch", sinkTaskKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    }

    /*****************************************
    *
    *  getDocumentID
    *
    *****************************************/

    @Override public String getDocumentID(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/

      Object subscriberProfileValue = sinkRecord.value();
      Schema subscriberProfileValueSchema = sinkRecord.valueSchema();
      SubscriberProfile subscriberProfile = SubscriberProfile.unpack(new SchemaAndValue(subscriberProfileValueSchema, subscriberProfileValue));

      /****************************************
      *
      *  use subscriberID
      *
      ****************************************/

      return subscriberProfile.getSubscriberID();
    }

    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/

    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/

      Object subscriberProfileValue = sinkRecord.value();
      Schema subscriberProfileValueSchema = sinkRecord.valueSchema();
      SubscriberProfile subscriberProfile = SubscriberProfile.unpack(new SchemaAndValue(subscriberProfileValueSchema, subscriberProfileValue));

      /*****************************************
      *
      *  context
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("evaluationDate", now);
      documentMap.put("subscriptionDate", subscriberProfile.getActivationDate());
      documentMap.put("contractID", subscriberProfile.getContractID());
      documentMap.put("accountTypeID", subscriberProfile.getAccountTypeID());
      documentMap.put("ratePlan", subscriberProfile.getRatePlan());
      documentMap.put("ratePlanChangeDate", subscriberProfile.getRatePlanChangeDate());
      documentMap.put("subscriberStatus", (subscriberProfile.getSubscriberStatus() != null) ? subscriberProfile.getSubscriberStatus().getStringRepresentation() : null);
      documentMap.put("previousSubscriberStatus", (subscriberProfile.getPreviousSubscriberStatus() != null) ? subscriberProfile.getPreviousSubscriberStatus().getStringRepresentation() : null);
      documentMap.put("statusChangeDate", subscriberProfile.getStatusChangeDate());
      documentMap.put("universalControlGroup", subscriberProfile.getControlGroup(subscriberGroupEpochReader));
      documentMap.put("controlGroup", subscriberProfile.getUniversalControlGroup(subscriberGroupEpochReader));
      documentMap.put("language", subscriberProfile.getLanguage() != null ? subscriberProfile.getLanguage() : Deployment.getBaseLanguage());
      documentMap.put("region", subscriberProfile.getRegion());
      documentMap.put("subscriberGroups", subscriberProfile.getSubscriberGroups(subscriberGroupEpochReader));
      documentMap.put("totalChargeYesterday", subscriberProfile.getHistoryTotalChargeYesterday(now));
      documentMap.put("totalChargePrevious7Days", subscriberProfile.getHistoryTotalChargePrevious7Days(now));
      documentMap.put("totalChargePrevious14Days", subscriberProfile.getHistoryTotalChargePrevious14Days(now));
      documentMap.put("totalChargePreviousMonth", subscriberProfile.getHistoryTotalChargePreviousMonth(now));
      documentMap.put("rechargeCountYesterday", subscriberProfile.getHistoryRechargeCountYesterday(now));
      documentMap.put("rechargeCountPrevious7Days", subscriberProfile.getHistoryRechargeCountPrevious7Days(now));
      documentMap.put("rechargeCountPrevious14Days", subscriberProfile.getHistoryRechargeCountPrevious14Days(now));
      documentMap.put("rechargeCountPreviousMonth", subscriberProfile.getHistoryRechargeCountPreviousMonth(now));
      documentMap.put("rechargeChargeYesterday", subscriberProfile.getHistoryRechargeChargeYesterday(now));
      documentMap.put("rechargeChargePrevious7Days", subscriberProfile.getHistoryRechargeChargePrevious7Days(now));
      documentMap.put("rechargeChargePrevious14Days", subscriberProfile.getHistoryRechargeChargePrevious14Days(now));
      documentMap.put("rechargeChargePreviousMonth", subscriberProfile.getHistoryRechargeChargePreviousMonth(now));
      documentMap.put("lastRechargeDate", subscriberProfile.getLastRechargeDate());
      documentMap.put("mainBalanceValue", subscriberProfile.getMainBalanceValue());
      documentMap.put("moCallChargeYesterday", subscriberProfile.getHistoryMOCallChargeYesterday(now));
      documentMap.put("moCallChargePrevious7Days", subscriberProfile.getHistoryMOCallChargePrevious7Days(now));
      documentMap.put("moCallChargePrevious14Days", subscriberProfile.getHistoryMOCallChargePrevious14Days(now));
      documentMap.put("moCallChargePreviousMonth", subscriberProfile.getHistoryMOCallChargePreviousMonth(now));
      documentMap.put("moCallCountYesterday", subscriberProfile.getHistoryMOCallCountYesterday(now));
      documentMap.put("moCallCountPrevious7Days", subscriberProfile.getHistoryMOCallCountPrevious7Days(now));
      documentMap.put("moCallCountPrevious14Days", subscriberProfile.getHistoryMOCallCountPrevious14Days(now));
      documentMap.put("moCallCountPreviousMonth", subscriberProfile.getHistoryMOCallCountPreviousMonth(now));
      documentMap.put("moCallDurationYesterday", subscriberProfile.getHistoryMOCallDurationYesterday(now));
      documentMap.put("moCallDurationPrevious7Days", subscriberProfile.getHistoryMOCallDurationPrevious7Days(now));
      documentMap.put("moCallDurationPrevious14Days", subscriberProfile.getHistoryMOCallDurationPrevious14Days(now));
      documentMap.put("moCallDurationPreviousMonth", subscriberProfile.getHistoryMOCallDurationPreviousMonth(now));
      documentMap.put("mtCallCountYesterday", subscriberProfile.getHistoryMTCallCountYesterday(now));
      documentMap.put("mtCallCountPrevious7Days", subscriberProfile.getHistoryMTCallCountPrevious7Days(now));
      documentMap.put("mtCallCountPrevious14Days", subscriberProfile.getHistoryMTCallCountPrevious14Days(now));
      documentMap.put("mtCallCountPreviousMonth", subscriberProfile.getHistoryMTCallCountPreviousMonth(now));
      documentMap.put("mtCallDurationYesterday", subscriberProfile.getHistoryMTCallDurationYesterday(now));
      documentMap.put("mtCallDurationPrevious7Days", subscriberProfile.getHistoryMTCallDurationPrevious7Days(now));
      documentMap.put("mtCallDurationPrevious14Days", subscriberProfile.getHistoryMTCallDurationPrevious14Days(now));
      documentMap.put("mtCallDurationPreviousMonth", subscriberProfile.getHistoryMTCallDurationPreviousMonth(now));
      documentMap.put("mtCallIntCountYesterday", subscriberProfile.getHistoryMTCallIntCountYesterday(now));
      documentMap.put("mtCallIntCountPrevious7Days", subscriberProfile.getHistoryMTCallIntCountPrevious7Days(now));
      documentMap.put("mtCallIntCountPrevious14Days", subscriberProfile.getHistoryMTCallIntCountPrevious14Days(now));
      documentMap.put("mtCallIntCountPreviousMonth", subscriberProfile.getHistoryMTCallIntCountPreviousMonth(now));
      documentMap.put("mtCallIntDurationYesterday", subscriberProfile.getHistoryMTCallIntDurationYesterday(now));
      documentMap.put("mtCallIntDurationPrevious7Days", subscriberProfile.getHistoryMTCallIntDurationPrevious7Days(now));
      documentMap.put("mtCallIntDurationPrevious14Days", subscriberProfile.getHistoryMTCallIntDurationPrevious14Days(now));
      documentMap.put("mtCallIntDurationPreviousMonth", subscriberProfile.getHistoryMTCallIntDurationPreviousMonth(now));
      documentMap.put("moCallIntChargeYesterday", subscriberProfile.getHistoryMOCallIntChargeYesterday(now));
      documentMap.put("moCallIntChargePrevious7Days", subscriberProfile.getHistoryMOCallIntChargePrevious7Days(now));
      documentMap.put("moCallIntChargePrevious14Days", subscriberProfile.getHistoryMOCallIntChargePrevious14Days(now));
      documentMap.put("moCallIntChargePreviousMonth", subscriberProfile.getHistoryMOCallIntChargePreviousMonth(now));
      documentMap.put("moCallIntCountYesterday", subscriberProfile.getHistoryMOCallIntCountYesterday(now));
      documentMap.put("moCallIntCountPrevious7Days", subscriberProfile.getHistoryMOCallIntCountPrevious7Days(now));
      documentMap.put("moCallIntCountPrevious14Days", subscriberProfile.getHistoryMOCallIntCountPrevious14Days(now));
      documentMap.put("moCallIntCountPreviousMonth", subscriberProfile.getHistoryMOCallIntCountPreviousMonth(now));
      documentMap.put("moCallIntDurationYesterday", subscriberProfile.getHistoryMOCallIntDurationYesterday(now));
      documentMap.put("moCallIntDurationPrevious7Days", subscriberProfile.getHistoryMOCallIntDurationPrevious7Days(now));
      documentMap.put("moCallIntDurationPrevious14Days", subscriberProfile.getHistoryMOCallIntDurationPrevious14Days(now));
      documentMap.put("moCallIntDurationPreviousMonth", subscriberProfile.getHistoryMOCallIntDurationPreviousMonth(now));
      documentMap.put("moSMSChargeYesterday", subscriberProfile.getHistoryMOSMSChargeYesterday(now));
      documentMap.put("moSMSChargePrevious7Days", subscriberProfile.getHistoryMOSMSChargePrevious7Days(now));
      documentMap.put("moSMSChargePrevious14Days", subscriberProfile.getHistoryMOSMSChargePrevious14Days(now));
      documentMap.put("moSMSChargePreviousMonth", subscriberProfile.getHistoryMOSMSChargePreviousMonth(now));
      documentMap.put("moSMSCountYesterday", subscriberProfile.getHistoryMOSMSCountYesterday(now));
      documentMap.put("moSMSCountPrevious7Days", subscriberProfile.getHistoryMOSMSCountPrevious7Days(now));
      documentMap.put("moSMSCountPrevious14Days", subscriberProfile.getHistoryMOSMSCountPrevious14Days(now));
      documentMap.put("moSMSCountPreviousMonth", subscriberProfile.getHistoryMOSMSCountPreviousMonth(now));
      documentMap.put("dataVolumeYesterday", subscriberProfile.getHistoryDataVolumeYesterday(now));
      documentMap.put("dataVolumePrevious7Days", subscriberProfile.getHistoryDataVolumePrevious7Days(now));
      documentMap.put("dataVolumePrevious14Days", subscriberProfile.getHistoryDataVolumePrevious14Days(now));
      documentMap.put("dataVolumePreviousMonth", subscriberProfile.getHistoryDataVolumePreviousMonth(now));
      documentMap.put("dataBundleChargeYesterday", subscriberProfile.getHistoryDataBundleChargeYesterday(now));
      documentMap.put("dataBundleChargePrevious7Days", subscriberProfile.getHistoryDataBundleChargePrevious7Days(now));
      documentMap.put("dataBundleChargePrevious14Days", subscriberProfile.getHistoryDataBundleChargePrevious14Days(now));
      documentMap.put("dataBundleChargePreviousMonth", subscriberProfile.getHistoryDataBundleChargePreviousMonth(now));
      
      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
