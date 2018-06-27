/*****************************************************************************
*
*  Deployment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;

import com.evolving.nglm.core.ServerRuntimeException;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Deployment
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static String journeyTopic;
  private static String offerTopic;
  private static String subscriberUpdateTopic;
  private static String externalAggregatesTopic;
  private static String externalAggregatesAssignSubscriberIDTopic;
  private static String subscriberGroupTopic;
  private static String subscriberGroupAssignSubscriberIDTopic;
  private static String subscriberGroupEpochTopic;
  private static String subscriberTraceControlTopic;
  private static String subscriberTraceTopic;
  private static String subscriberProfileChangeLog;
  private static String subscriberProfileChangeLogTopic;
  private static Map<String,SupportedLanguage> supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
  private static Map<String,SupportedCurrency> supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
  private static Map<String,SupportedTimeUnit> supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
  private static Map<String,SalesChannel> salesChannels = new LinkedHashMap<String,SalesChannel>();
  private static Map<String,SupportedDataType> supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
  private static Map<String,CriterionField> profileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,OfferType> offerTypes = new LinkedHashMap<String,OfferType>();
  private static Map<String,ProductType> productTypes = new LinkedHashMap<String,ProductType>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  //
  //  core accessors
  //
  
  public static String getZookeeperRoot() { return com.evolving.nglm.core.Deployment.getZookeeperRoot(); }
  public static String getZookeeperConnect() { return com.evolving.nglm.core.Deployment.getZookeeperConnect(); }
  public static String getBrokerServers() { return com.evolving.nglm.core.Deployment.getBrokerServers(); }
  public static JSONObject getJSONRoot() { return com.evolving.nglm.core.Deployment.getJSONRoot(); }
  public static String getBaseTimeZone() { return com.evolving.nglm.core.Deployment.getBaseTimeZone(); }
  public static String getBaseLanguage() { return com.evolving.nglm.core.Deployment.getBaseLanguage(); }
  public static String getBaseCountry() { return com.evolving.nglm.core.Deployment.getBaseCountry(); }
  public static String getRedisSentinels() { return com.evolving.nglm.core.Deployment.getRedisSentinels(); }
  public static String getRecordAlternateIDTopic() { return com.evolving.nglm.core.Deployment.getRecordAlternateIDTopic(); }
  
  //
  //  deployment accessors
  //

  public static String getJourneyTopic() { return journeyTopic; }
  public static String getOfferTopic() { return offerTopic; }
  public static String getSubscriberUpdateTopic() { return subscriberUpdateTopic; }
  public static String getExternalAggregatesTopic() { return externalAggregatesTopic; }
  public static String getExternalAggregatesAssignSubscriberIDTopic() { return externalAggregatesAssignSubscriberIDTopic; }
  public static String getSubscriberGroupTopic() { return subscriberGroupTopic; }
  public static String getSubscriberGroupAssignSubscriberIDTopic() { return subscriberGroupAssignSubscriberIDTopic; }
  public static String getSubscriberGroupEpochTopic() { return subscriberGroupEpochTopic; }
  public static String getSubscriberTraceControlTopic() { return subscriberTraceControlTopic; }
  public static String getSubscriberTraceTopic() { return subscriberTraceTopic; }
  public static String getSubscriberProfileChangeLog() { return subscriberProfileChangeLog; }
  public static String getSubscriberProfileChangeLogTopic() { return subscriberProfileChangeLogTopic; }
  public static Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public static Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public static Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public static Map<String,SalesChannel> getSalesChannels() { return salesChannels; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; }
  public static Map<String,OfferType> getOfferTypes() { return offerTypes; }
  public static Map<String,ProductType> getProductTypes() { return productTypes; }

  /*****************************************
  *
  *  getCriterionFields
  *
  *****************************************/
  
  public static Map<String,CriterionField> getCriterionFields(CriterionContext criterionContext)
  {
    Map<String,CriterionField> result;
    switch (criterionContext)
      {
        case Profile:
          result = profileCriterionFields;
          break;
        default:
          throw new ServerRuntimeException("unknown criterionContext: " + criterionContext);
      }
    return result;
  }
  
  /*****************************************
  *
  *  static initialization
  *
  *****************************************/
  
  static
  {
    /*****************************************
    *
    *  super class
    *
    *****************************************/

    JSONObject jsonRoot = com.evolving.nglm.core.Deployment.getJSONRoot();
    
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  journeyTopic
    //

    try
      {
        journeyTopic = JSONUtilities.decodeString(jsonRoot, "journeyTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  offerTopic
    //

    try
      {
        offerTopic = JSONUtilities.decodeString(jsonRoot, "offerTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  subscriberUpdateTopic
    //

    try
      {
        subscriberUpdateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberUpdateTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  externalAggregatesTopic
    //

    try
      {
        externalAggregatesTopic = JSONUtilities.decodeString(jsonRoot, "externalAggregatesTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  externalAggregatesAssignSubscriberIDTopic
    //

    try
      {
        externalAggregatesAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "externalAggregatesAssignSubscriberIDTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  subscriberGroupTopic
    //

    try
      {
        subscriberGroupTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  subscriberGroupAssignSubscriberIDTopic
    //

    try
      {
        subscriberGroupAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupAssignSubscriberIDTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  subscriberGroupEpochTopic
    //

    try
      {
        subscriberGroupEpochTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupEpochTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  subscriberTraceControlTopic
    //

    try
      {
        subscriberTraceControlTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  subscriberTraceTopic
    //

    try
      {
        subscriberTraceTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  subscriberProfileChangeLog
    //

    try
      {
        subscriberProfileChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberProfileChangeLog", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  subscriberProfileChangeLogTopic
    //

    try
      {
        subscriberProfileChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "subscriberProfileChangeLogTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  supportedLanguages
    //

    try
      {
        JSONArray supportedLanguageValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedLanguages", true);
        for (int i=0; i<supportedLanguageValues.size(); i++)
          {
            JSONObject supportedLanguageJSON = (JSONObject) supportedLanguageValues.get(i);
            SupportedLanguage supportedLanguage = new SupportedLanguage(supportedLanguageJSON);
            supportedLanguages.put(supportedLanguage.getSupportedLanguageName(), supportedLanguage);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  supportedCurrencies
    //

    try
      {
        JSONArray supportedCurrencyValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedCurrencies", true);
        for (int i=0; i<supportedCurrencyValues.size(); i++)
          {
            JSONObject supportedCurrencyJSON = (JSONObject) supportedCurrencyValues.get(i);
            SupportedCurrency supportedCurrency = new SupportedCurrency(supportedCurrencyJSON);
            supportedCurrencies.put(supportedCurrency.getSupportedCurrencyName(), supportedCurrency);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  supportedTimeUnits
    //

    try
      {
        JSONArray supportedTimeUnitValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedTimeUnits", true);
        for (int i=0; i<supportedTimeUnitValues.size(); i++)
          {
            JSONObject supportedTimeUnitJSON = (JSONObject) supportedTimeUnitValues.get(i);
            SupportedTimeUnit supportedTimeUnit = new SupportedTimeUnit(supportedTimeUnitJSON);
            supportedTimeUnits.put(supportedTimeUnit.getSupportedTimeUnitName(), supportedTimeUnit);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  salesChannels
    //

    try
      {
        JSONArray salesChannelValues = JSONUtilities.decodeJSONArray(jsonRoot, "salesChannels", true);
        for (int i=0; i<salesChannelValues.size(); i++)
          {
            JSONObject salesChannelJSON = (JSONObject) salesChannelValues.get(i);
            SalesChannel salesChannel = new SalesChannel(salesChannelJSON);
            salesChannels.put(salesChannel.getSalesChannelName(), salesChannel);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  supportedDataTypes
    //

    try
      {
        JSONArray supportedDataTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedDataTypes", true);
        for (int i=0; i<supportedDataTypeValues.size(); i++)
          {
            JSONObject supportedDataTypeJSON = (JSONObject) supportedDataTypeValues.get(i);
            SupportedDataType supportedDataType = new SupportedDataType(supportedDataTypeJSON);
            supportedDataTypes.put(supportedDataType.getCriterionDataTypeName(), supportedDataType);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  profileCriterionFields
    //

    try
      {
        JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriterionFields", true);
        for (int i=0; i<criterionFieldValues.size(); i++)
          {
            JSONObject criterionFieldJSON = (JSONObject) criterionFieldValues.get(i);
            CriterionField criterionField = new CriterionField(criterionFieldJSON);
            profileCriterionFields.put(criterionField.getCriterionFieldName(), criterionField);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  offerTypes
    //

    try
      {
        JSONArray offerTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerTypes", true);
        for (int i=0; i<offerTypeValues.size(); i++)
          {
            JSONObject offerTypeJSON = (JSONObject) offerTypeValues.get(i);
            OfferType offerType = new OfferType(offerTypeJSON);
            offerTypes.put(offerType.getOfferTypeName(), offerType);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  productTypes
    //

    try
      {
        JSONArray productTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "productTypes", true);
        for (int i=0; i<productTypeValues.size(); i++)
          {
            JSONObject productTypeJSON = (JSONObject) productTypeValues.get(i);
            ProductType productType = new ProductType(productTypeJSON);
            productTypes.put(productType.getProductTypeName(), productType);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
  }

  /*****************************************
  *
  *  main (for validation)
  *
  *****************************************/

  public static void main(String[] args)
  {
    System.out.println("zookeeper root: " + getZookeeperRoot());
  }

  /*****************************************
  *
  *  criterionFieldRetrievers
  *
  *****************************************/

  //
  //  system
  //

  public static Object getEvaluationDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getEvaluationDate(); }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return null; }

  //
  //  simple
  //
  
  public static Object getSubscriptionDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getActivationDate(); }
  public static Object getContractID(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getContractID(); }
  public static Object getAccountTypeID(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getAccountTypeID(); }
  public static Object getRatePlan(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getRatePlan(); }
  public static Object getRatePlanChangeDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getRatePlanChangeDate(); }
  public static Object getSubscriberStatus(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getSubscriberStatus().getStringRepresentation(); }
  public static Object getPreviousSubscriberStatus(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getPreviousSubscriberStatus().getStringRepresentation(); }
  public static Object getStatusChangeDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getControlGroup(evaluationRequest.getSubscriberGroupEpochReader()); }
  public static Object getControlGroup(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(evaluationRequest.getSubscriberGroupEpochReader()); }
  public static Object getLanguage(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getLanguage() != null ? evaluationRequest.getSubscriberProfile().getLanguage() : getBaseLanguage(); }
  public static Object getRegion(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getRegion(); }

  //
  //  subscriber group membership
  //

  public static Object getSubscriberGroups(SubscriberEvaluationRequest evaluationRequest) { return evaluationRequest.getSubscriberProfile().getSubscriberGroups(evaluationRequest.getSubscriberGroupEpochReader()); }
  
  //
  //  total charge metrics
  //
  
  public static Object getTotalChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryTotalChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getTotalChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryTotalChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getTotalChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryTotalChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getTotalChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryTotalChargePreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  recharge count metrics
  //
  
  public static Object getRechargeCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeCountPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  recharge charge metrics
  //
  
  public static Object getRechargeChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getRechargeChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryRechargeChargePreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  simple recharge information
  //
  
  public static Object getLastRechargeDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getLastRechargeDate(); }
  public static Object getMainBalanceValue(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getMainBalanceValue(); }

  //
  //  mo call charge metrics
  //
  
  public static Object getMOCallChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallChargePreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mo call count metrics
  //
  
  public static Object getMOCallCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallCountPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mo call duration metrics
  //
  
  public static Object getMOCallDurationYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallDurationYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallDurationPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallDurationPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallDurationPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallDurationPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallDurationPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallDurationPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mt call count metrics
  //
  
  public static Object getMTCallCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallCountPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mt call duration metrics
  //
  
  public static Object getMTCallDurationYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallDurationYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallDurationPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallDurationPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallDurationPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallDurationPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallDurationPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallDurationPreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  mt international call count metrics
  //
  
  public static Object getMTCallIntCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntCountPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mt international call duration metrics
  //
  
  public static Object getMTCallIntDurationYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntDurationYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntDurationPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntDurationPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntDurationPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntDurationPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMTCallIntDurationPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMTCallIntDurationPreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  mo international call charge metrics
  //
  
  public static Object getMOCallIntChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntChargePreviousMonth(evaluationRequest.getEvaluationDate()); }

  
  //
  //  mo international call count metrics
  //
  
  public static Object getMOCallIntCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntCountPreviousMonth(evaluationRequest.getEvaluationDate()); }

  //
  //  mo international call duration metrics
  //
  
  public static Object getMOCallIntDurationYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntDurationYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntDurationPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntDurationPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntDurationPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntDurationPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOCallIntDurationPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOCallIntDurationPreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  mo sms charge metrics
  //
  
  public static Object getMOSMSChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSChargePreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  mo sms count metrics
  //
  
  public static Object getMOSMSCountYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSCountYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSCountPrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSCountPrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSCountPrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSCountPrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getMOSMSCountPreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryMOSMSCountPreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  data volume metrics
  //
  
  public static Object getDataVolumeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataVolumeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getDataVolumePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataVolumePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getDataVolumePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataVolumePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getDataVolumePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataVolumePreviousMonth(evaluationRequest.getEvaluationDate()); }
  
  //
  //  data bundle charge metrics
  //
  
  public static Object getDataBundleChargeYesterday(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataBundleChargeYesterday(evaluationRequest.getEvaluationDate()); }
  public static Object getDataBundleChargePrevious7Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataBundleChargePrevious7Days(evaluationRequest.getEvaluationDate()); }
  public static Object getDataBundleChargePrevious14Days(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataBundleChargePrevious14Days(evaluationRequest.getEvaluationDate()); }
  public static Object getDataBundleChargePreviousMonth(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getHistoryDataBundleChargePreviousMonth(evaluationRequest.getEvaluationDate()); }
}
