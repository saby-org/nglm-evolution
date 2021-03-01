/****************************************************************************
*
*  Deployment.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.BillingMode;
import com.evolving.nglm.evolution.CommunicationChannelTimeWindow;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.PropensityRule;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.SupportedCurrency;
import com.evolving.nglm.evolution.SupportedLanguage;
import com.evolving.nglm.evolution.SupportedTimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 
 * This class should not contain any static variable. 
 * All settings put here can be override by each tenant.
 *
 * Deployment.getDefault().getX() allow to retrieve the default value of one setting (from tenant 0).
 * Deployment.getDeployment(4).getX() allow to retrieve the specific value of one setting for tenant 4 (retrieve the default one otherwise) 
 */
public class Deployment extends DeploymentCommon
{
  /*****************************************
  *
  * Static data
  *
  *****************************************/
  protected static final Logger log = LoggerFactory.getLogger(Deployment.class);
  
  private static Map<Integer, Deployment> deploymentsPerTenant = new HashMap<>();
  private static Object lock = new Object();

  /*****************************************
  *
  * Static accessors
  *
  *****************************************/
  
  public static Set<Integer> getTenantIDs() { return jsonConfigPerTenant.keySet(); }
  // TODO EVPRO-99 rl : la plupart du temps quand on call getDefault ça signifie qu'on ne regardera jamais l'info specifique tenant
  // Il faut donc empecher quelle soit override par le tenant inutilement : Lacher un warning
  public static Deployment getDefault() { return getDeployment(0); }
  
  // TODO EVPRO-99 ok but, we will load all of them every time at the beginning no matter what.. (example datacubes manager load every tenant..)
  public static Deployment getDeployment(int tenantID)
  {
    Deployment result = deploymentsPerTenant.get(tenantID);
    if(result == null)
      {
        synchronized(lock)
          {
            result = deploymentsPerTenant.get(tenantID);
            if(result == null)
              {
                /////// TOOOOOOOOOOOODOOOOOOOO MOVE THIS OUT, INIT AT THE BEGINING, THROW - CRASH EVERYTHING
                try
                  {
                    result = new Deployment(tenantID);
                  } catch (Exception e)
                  {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  }
                deploymentsPerTenant.put(tenantID, result);
              }
          }
      }
    return deploymentsPerTenant.get(tenantID);
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  //
  // Local environment
  //
  private String timeZone;
  private ZoneId zoneId;
  private String language;
  private String country;
  private Map<String,SupportedLanguage> supportedLanguages;
  private String baseLanguageID;
  
  //
  // Elasticsearch
  //
  private int elasticsearchRetentionDaysJourneys;
  private int elasticsearchRetentionDaysCampaigns;
  private int elasticsearchRetentionDaysBulkCampaigns;
  private int elasticsearchRetentionDaysExpiredVouchers; 
  private Map<String,ScheduledJobConfiguration> datacubeJobsScheduling;
  private Map<String,ScheduledJobConfiguration> elasticsearchJobsScheduling;
  
  //
  // Others
  //
  private CommunicationChannelTimeWindow defaultNotificationTimeWindowsMap;
  private Map<String,BillingMode> billingModes;
  private PropensityRule propensityRule;
  private Map<String,SupportedCurrency> supportedCurrencies;
  private Map<String,SupportedTimeUnit> supportedTimeUnits;
  private static List<EvaluationCriterion> journeyUniversalEligibilityCriteria;
  
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/  
  //
  // Local environment
  //
  public String getTimeZone() { return timeZone; }
  public ZoneId getZoneId() { return zoneId; }
  public Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public String getSupportedLanguageID(String language){ return Deployment.getSupportedLanguageID(language, getSupportedLanguages()); }
  public String getLanguage() { return language; }
  public String getLanguageID() { return baseLanguageID; }
  public String getCountry() { return country; }
  
  //
  // Elasticsearch
  //
  public int getElasticsearchRetentionDaysJourneys() { return elasticsearchRetentionDaysJourneys; }
  public int getElasticsearchRetentionDaysCampaigns() { return elasticsearchRetentionDaysCampaigns; }
  public int getElasticsearchRetentionDaysBulkCampaigns() { return elasticsearchRetentionDaysBulkCampaigns; }
  public int getElasticsearchRetentionDaysExpiredVouchers() { return elasticsearchRetentionDaysExpiredVouchers; }  
  public Map<String,ScheduledJobConfiguration> getDatacubeJobsScheduling() { return datacubeJobsScheduling; }
  public Map<String,ScheduledJobConfiguration> getElasticsearchJobsScheduling() { return elasticsearchJobsScheduling; }
  
  //
  // Others
  //
  public CommunicationChannelTimeWindow getDefaultNotificationDailyWindows() { return defaultNotificationTimeWindowsMap; }
  public Map<String,BillingMode> getBillingModes() { return billingModes; }
  public PropensityRule getPropensityRule() { return propensityRule; }
  public Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public List<EvaluationCriterion> getJourneyUniversalEligibilityCriteria() { return journeyUniversalEligibilityCriteria; } 
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/  
  public Deployment(int tenantID) throws Exception
  {    
    load(tenantID);
    /*
    try
      {
      } 
    catch (RuntimeException|NoSuchMethodException|IllegalAccessException|ClassNotFoundException|GUIManagerException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Error while loading Deployment from JSON. {}", stackTraceWriter.toString());
      } */
  }
  
  private void load(int tenantID) throws Exception
  { 
    JSONObject jsonRoot = jsonConfigPerTenant.get(tenantID);
    
    //System.out.println(jsonRoot.toJSONString());
    
    //
    // Local information
    //
    
    timeZone = JSONUtilities.decodeString(jsonRoot, "timeZone", true);
    zoneId = ZoneId.of(timeZone);
    language = JSONUtilities.decodeString(jsonRoot, "language", true);
    country = JSONUtilities.decodeString(jsonRoot, "country", true);
    // supportedLanguages
    supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
    JSONArray supportedLanguageValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedLanguages", true);
    for (int i=0; i<supportedLanguageValues.size(); i++)
      {
        JSONObject supportedLanguageJSON = (JSONObject) supportedLanguageValues.get(i);
        SupportedLanguage supportedLanguage = new SupportedLanguage(supportedLanguageJSON);
        supportedLanguages.put(supportedLanguage.getID(), supportedLanguage);
      }
    baseLanguageID = getSupportedLanguageID(getLanguage(), supportedLanguages);
    
    //
    // Elasticsearch settings
    //
    elasticsearchRetentionDaysJourneys = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysJourneys", true);
    elasticsearchRetentionDaysCampaigns = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysCampaigns", true);
    elasticsearchRetentionDaysBulkCampaigns = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysBulkCampaigns", true);
    elasticsearchRetentionDaysExpiredVouchers = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysExpiredVouchers", true);
    
    // Datacubes jobs
    if(tenantID == 0) {
      datacubeJobsScheduling = null; // because datacube jobs make no sense for "tenant 0".
    } else {
      datacubeJobsScheduling = new LinkedHashMap<String,ScheduledJobConfiguration>();
      JSONObject datacubeJobsSchedulingJSON = JSONUtilities.decodeJSONObject(jsonRoot, "datacubeJobsScheduling", true);
      for (Object key : datacubeJobsSchedulingJSON.keySet()) {
        datacubeJobsScheduling.put((String) key, new ScheduledJobConfiguration((String) key, tenantID, (JSONObject) datacubeJobsSchedulingJSON.get(key)));
      }
    }
    
    // Elasticsearch jobs
    if(tenantID == 0) {
      elasticsearchJobsScheduling = null; // because elasticsearch jobs make no sense for "tenant 0".
    } else {
      elasticsearchJobsScheduling = new LinkedHashMap<String,ScheduledJobConfiguration>();
      JSONObject elasticsearchJobsSchedulingJSON = JSONUtilities.decodeJSONObject(jsonRoot, "elasticsearchJobsScheduling", true);
      for (Object key : elasticsearchJobsSchedulingJSON.keySet()) {
        elasticsearchJobsScheduling.put((String) key, new ScheduledJobConfiguration((String) key, tenantID, (JSONObject) elasticsearchJobsSchedulingJSON.get(key)));
      }
    }
    
    //
    // Others 
    //
  
    //  notificationDailyWindows
    JSONObject defaultTimeWindowJSON = (JSONObject) jsonRoot.get("notificationDailyWindows");
    if(defaultTimeWindowJSON != null)
      {
        defaultTimeWindowJSON.put("id", "default");
        defaultTimeWindowJSON.put("name", "default");
        defaultTimeWindowJSON.put("display", "default");
        defaultTimeWindowJSON.put("active", true);
        defaultTimeWindowJSON.put("communicationChannelID", "default");
      }
    defaultNotificationTimeWindowsMap = new CommunicationChannelTimeWindow(defaultTimeWindowJSON, System.currentTimeMillis() * 1000, null, tenantID);
  
    //  billingModes
    billingModes = new LinkedHashMap<String,BillingMode>();
    JSONArray billingModeValues = JSONUtilities.decodeJSONArray(jsonRoot, "billingModes", true);
    for (int i=0; i<billingModeValues.size(); i++)
      {
        JSONObject billingModesJSON = (JSONObject) billingModeValues.get(i);
        BillingMode billingMode = new BillingMode(billingModesJSON);
        billingModes.put(billingMode.getID(), billingMode);
      }
    
    propensityRule = new PropensityRule((JSONObject) jsonRoot.get("propensityRule"));
  
    //  supportedCurrencies
    supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
    JSONArray supportedCurrencyValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedCurrencies", true);
    for (int i=0; i<supportedCurrencyValues.size(); i++)
      {
        JSONObject supportedCurrencyJSON = (JSONObject) supportedCurrencyValues.get(i);
        SupportedCurrency supportedCurrency = new SupportedCurrency(supportedCurrencyJSON);
        supportedCurrencies.put(supportedCurrency.getID(), supportedCurrency);
      }
    
    //  supportedTimeUnits
    supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
    JSONArray supportedTimeUnitValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedTimeUnits", true);
    for (int i=0; i<supportedTimeUnitValues.size(); i++)
      {
        JSONObject supportedTimeUnitJSON = (JSONObject) supportedTimeUnitValues.get(i);
        SupportedTimeUnit supportedTimeUnit = new SupportedTimeUnit(supportedTimeUnitJSON);
        supportedTimeUnits.put(supportedTimeUnit.getID(), supportedTimeUnit);
      }

    //  journeyUniversalEligibilityCriteria
    journeyUniversalEligibilityCriteria = new ArrayList<>();
    JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyUniversalEligibilityCriteria", new JSONArray());
    for (int i=0; i<evaluationCriterionValues.size(); i++)
      {
        JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
        EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile(tenantID), tenantID);
        getJourneyUniversalEligibilityCriteria().add(evaluationCriterion);                  
      }
  }
}
