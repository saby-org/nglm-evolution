/****************************************************************************
*
*  Deployment.java
*
****************************************************************************/

package com.evolving.nglm.core;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Deployment
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static JSONObject jsonRoot;
  private static String baseTimeZone;
  private static ZoneId baseZoneId;
  private static String baseLanguage;
  private static String baseCountry;
  private static Map<String,AlternateID> alternateIDs = new LinkedHashMap<String,AlternateID>();
  private static String assignSubscriberIDsTopic;
  private static String assignExternalSubscriberIDsTopic;
  private static String updateExternalSubscriberIDTopic;
  private static String recordSubscriberIDTopic;
  private static String recordAlternateIDTopic;
  private static String autoProvisionedSubscriberChangeLog;
  private static String autoProvisionedSubscriberChangeLogTopic;
  private static String rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic;
  private static String cleanupSubscriberTopic;
  private static Set<String> cleanupSubscriberElasticsearchIndexes = new HashSet<String>();
  private static String externalSubscriberID;
  private static String subscriberTraceControlAlternateID;
  private static boolean subscriberTraceControlAutoProvision;
  private static String subscriberTraceControlTopic;
  private static String subscriberTraceControlAssignSubscriberIDTopic;
  private static String subscriberTraceTopic;
  private static String simulatedTimeTopic;
  private static Map<String,AutoProvisionEvent> autoProvisionEvents = new LinkedHashMap<String,AutoProvisionEvent>();
  // ELASTICSEARCH (moved in core because it's needed for setup/upgrade process)
  private static int elasticsearchConnectTimeout;
  private static int elasticsearchQueryTimeout;
  private static int elasticsearchScrollSize;
  private static String elasticsearchDateFormat;
  private static int elasticsearchDefaultShards;
  private static int elasticsearchDefaultReplicas;
  private static int elasticsearchSubscriberprofileShards;
  private static int elasticsearchSubscriberprofileReplicas;
  private static int elasticsearchSnapshotShards;
  private static int elasticsearchSnapshotReplicas;
  private static int elasticsearchLiveVoucherShards;
  private static int elasticsearchLiveVoucherReplicas;
  private static int elasticsearchRetentionDaysODR;
  private static int elasticsearchRetentionDaysBDR;
  private static int elasticsearchRetentionDaysMDR;
  private static int elasticsearchRetentionDaysTokens;
  private static int elasticsearchRetentionDaysSnapshots;
  private static int elasticsearchRetentionDaysJourneys;
  private static int elasticsearchRetentionDaysCampaigns;
  private static int elasticsearchRetentionDaysBulkCampaigns;
  private static int elasticsearchRetentionDaysExpiredVouchers; 
  private static int elasticsearchRetentionDaysVDR;

  //
  //  accessors
  //

  public static String getZookeeperRoot() { return System.getProperty("nglm.zookeeper.root"); }
  public static String getZookeeperConnect() { return System.getProperty("zookeeper.connect"); }
  public static String getBrokerServers() { return System.getProperty("broker.servers",""); }
  public static JSONObject getJSONRoot() { return jsonRoot; }
  public static String getBaseTimeZone() { return baseTimeZone; }
  public static ZoneId getBaseZoneId() { return baseZoneId; }
  public static String getBaseLanguage() { return baseLanguage; }
  public static String getBaseCountry() { return baseCountry; }
  public static String getRedisSentinels() { return System.getProperty("redis.sentinels",""); }
  public static Map<String,AlternateID> getAlternateIDs() { return alternateIDs; }
  public static String getAssignSubscriberIDsTopic() { return assignSubscriberIDsTopic; }
  public static String getAssignExternalSubscriberIDsTopic() { return assignExternalSubscriberIDsTopic; }
  public static String getUpdateExternalSubscriberIDTopic() { return updateExternalSubscriberIDTopic; }
  public static String getRecordSubscriberIDTopic() { return recordSubscriberIDTopic; }
  public static String getRecordAlternateIDTopic() { return recordAlternateIDTopic; }
  public static String getAutoProvisionedSubscriberChangeLog() { return autoProvisionedSubscriberChangeLog; }
  public static String getAutoProvisionedSubscriberChangeLogTopic() { return autoProvisionedSubscriberChangeLogTopic; }
  public static String getExternalSubscriberID() { return externalSubscriberID; }
  public static String getRekeyedAutoProvisionedAssignSubscriberIDsStreamTopic() { return rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic; }
  public static String getCleanupSubscriberTopic() { return cleanupSubscriberTopic; }
  public static String getSubscriberTraceControlAlternateID() { return subscriberTraceControlAlternateID; }
  public static boolean getSubscriberTraceControlAutoProvision() { return subscriberTraceControlAutoProvision; }
  public static String getSubscriberTraceControlTopic() { return subscriberTraceControlTopic; }
  public static String getSubscriberTraceControlAssignSubscriberIDTopic() { return subscriberTraceControlAssignSubscriberIDTopic; }
  public static String getSubscriberTraceTopic() { return subscriberTraceTopic; }
  public static String getSimulatedTimeTopic() { return simulatedTimeTopic; }
  public static Map<String,AutoProvisionEvent> getAutoProvisionEvents() { return autoProvisionEvents; }

  // ELASTICSEARCH 
  public static String getElasticsearchDateFormat() { return elasticsearchDateFormat; }
  public static int getElasticsearchScrollSize() {return elasticsearchScrollSize; }
  public static int getElasticsearchDefaultShards() { return elasticsearchDefaultShards; }
  public static int getElasticsearchDefaultReplicas() { return elasticsearchDefaultReplicas; }
  public static int getElasticsearchSubscriberprofileShards() { return elasticsearchSubscriberprofileShards; }
  public static int getElasticsearchSubscriberprofileReplicas() { return elasticsearchSubscriberprofileReplicas; }
  public static int getElasticsearchSnapshotShards() { return elasticsearchSnapshotShards; }
  public static int getElasticsearchSnapshotReplicas() { return elasticsearchSnapshotReplicas; }
  public static int getElasticsearchLiveVoucherShards() { return elasticsearchLiveVoucherShards; }
  public static int getElasticsearchLiveVoucherReplicas() { return elasticsearchLiveVoucherReplicas; }
  public static int getElasticsearchRetentionDaysODR() { return elasticsearchRetentionDaysODR; }
  public static int getElasticsearchRetentionDaysBDR() { return elasticsearchRetentionDaysBDR; }
  public static int getElasticsearchRetentionDaysMDR() { return elasticsearchRetentionDaysMDR; }
  public static int getElasticsearchRetentionDaysTokens() { return elasticsearchRetentionDaysTokens; }
  public static int getElasticsearchRetentionDaysSnapshots() { return elasticsearchRetentionDaysSnapshots; }
  public static int getElasticsearchRetentionDaysJourneys() { return elasticsearchRetentionDaysJourneys; }
  public static int getElasticsearchRetentionDaysCampaigns() { return elasticsearchRetentionDaysCampaigns; }
  public static int getElasticsearchRetentionDaysBulkCampaigns() { return elasticsearchRetentionDaysBulkCampaigns; }
  public static int getElasticsearchRetentionDaysExpiredVouchers() { return elasticsearchRetentionDaysExpiredVouchers; }  
  public static Set<String> getCleanupSubscriberElasticsearchIndexes() { return cleanupSubscriberElasticsearchIndexes; }
  public static int getElasticsearchRetentionDaysVDR() { return elasticsearchRetentionDaysVDR; }
  
  /*****************************************
  *
  *  static intialization
  *
  *****************************************/

  private static final Logger log = LoggerFactory.getLogger(Deployment.class);
  
  static
  {
    /*****************************************
    *
    *  zookeeper -- retrieve configuration
    *
    *****************************************/

    //
    //  create a client
    // 

    ZooKeeper zookeeper = null;
    while (zookeeper == null)
      {
        try
          {
            zookeeper = new ZooKeeper(System.getProperty("zookeeper.connect"), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, true);
          }
        catch (IOException e)
          {
            // ignore
          }
      }
    
    //
    //  ensure connected
    //

    while (zookeeper.getState().isAlive() && ! zookeeper.getState().isConnected())
      {
        try { Thread.currentThread().sleep(200); } catch (InterruptedException ie) { }
      }

    //
    //  verify connected
    //

    if (! zookeeper.getState().isConnected())
      {
        throw new RuntimeException("deployment");
      }



    DeploymentConfiguration deploymentConfiguration = null;
    TreeMap<String, DeploymentConfiguration> additionalDeploymentConfigurations = new TreeMap<String, DeploymentConfiguration>();
    TreeMap<String, DeploymentConfiguration> productDeploymentConfigurations = new TreeMap<String, DeploymentConfiguration>();
    
    String localDeploymentFiles = System.getProperty("deployment.repository");
    if(localDeploymentFiles != null) {
      // for development environment, don't get deployment*.json from Zookeeper, but from local disk
      File repository = new File(localDeploymentFiles);
      //      deployment.json
      
      //      deployment-templates.json
      //      deployment-toolbox.json
            
      //      deployment-product-evolution.json
      //      deployment-product-toolbox.json

      if(repository.exists()) {
        for(File f : repository.listFiles()) {
          if(f.getName().equals("deployment.json")) {
            // deployment.json
            try
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process("deployment", content.getBytes(), true);
                if (deploymentConfiguration == null)
                  {
                    deploymentConfiguration = new DeploymentConfiguration(part);
                  }
                else
                  {
                    deploymentConfiguration.setPart(part);
                  }
                
              } 
            catch (IOException e) 
              {
                e.printStackTrace();
              }
          }
          else if(f.getName().startsWith("deployment-") && !f.getName().contains("-product") && f.getName().endsWith(".json")) {
            // other that are NOT product-* so by example deployment-templates.json
            try 
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process(f.getName().substring("deployment-".length(),f.getName().indexOf(".json")), content.getBytes(), false);
                DeploymentConfiguration additionalDeploymentConfiguration = additionalDeploymentConfigurations.get(part.getBaseName());
                if (additionalDeploymentConfiguration == null)
                  {
                    additionalDeploymentConfiguration = new DeploymentConfiguration(part);
                    additionalDeploymentConfigurations.put(part.getBaseName(), additionalDeploymentConfiguration);
                  }
                else
                  {
                    additionalDeploymentConfiguration.setPart(part);
                  }
              }
            catch (IOException e) 
              {
                e.printStackTrace();
              }            
          }
          else if(f.getName().startsWith("deployment-product") && f.getName().endsWith(".json")) {
            //
            try 
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                String baseName = f.getName().substring(0,f.getName().indexOf(".json"));
                baseName = baseName.substring("deployment-product-".length(), baseName.length());
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process(baseName, content.getBytes(), false);
                DeploymentConfiguration productDeploymentConfiguration = productDeploymentConfigurations.get(part.getBaseName());
                if (productDeploymentConfiguration == null)
                {
                  productDeploymentConfiguration = new DeploymentConfiguration(part);
                  productDeploymentConfigurations.put(part.getBaseName(), productDeploymentConfiguration);
                }
                else
                {
                  productDeploymentConfiguration.setPart(part);
                }
              }
            catch (IOException e) 
              {
                e.printStackTrace();
              }  
          }
        }
      }
      else {
        log.warn("Deployment repository gotten from System.getProperties deployment.repository " + localDeploymentFiles + " does not exist");
        throw new RuntimeException("Deployment repository gotten from System.getProperties deployment.repository " + localDeploymentFiles + " does not exist");
      }
      
    }
    else {
      
      //
      //  read configuration from zookeeper (this load file of deployment.json, which can be split in sub parts)
      //
      try
        {
          for (String node : zookeeper.getChildren(getZookeeperRoot(), null, null))
            {
              if(log.isDebugEnabled()) log.debug("checking for base conf "+node);
              if (! node.startsWith("deployment")) continue;
              byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/" + node, null, null);
              if (bytes == null || bytes.length <= 1) continue;
              DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node, bytes, true);
              if (part == null) continue;
              if (deploymentConfiguration == null)
                {
                  deploymentConfiguration = new DeploymentConfiguration(part);
                }
              else
                {
                  deploymentConfiguration.setPart(part);
                }
              if(log.isDebugEnabled()) log.debug("adding base conf read "+node);
            }
        }
      catch (KeeperException|InterruptedException e)
        {
          throw new RuntimeException("deployment", e);
        }
      
      //
      //  read additional configuration from zookeeper (this load file of deployment-xxx.json, which can be splited in sub parts)
      //
  
      try
        {
          for (String node : zookeeper.getChildren(getZookeeperRoot() + "/deployment", null, null))
            {
              if(log.isDebugEnabled()) log.debug("checking for additional conf "+node);
              if (node.startsWith("product-")) continue;//skip the product ones
              byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/deployment/" + node, null, null);
              if (bytes == null || bytes.length <= 1) continue;
              DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node, bytes, false);
              if (part == null) continue;
              DeploymentConfiguration additionalDeploymentConfiguration = additionalDeploymentConfigurations.get(part.getBaseName());
              if (additionalDeploymentConfiguration == null)
                {
                  additionalDeploymentConfiguration = new DeploymentConfiguration(part);
                  additionalDeploymentConfigurations.put(part.getBaseName(), additionalDeploymentConfiguration);
                }
              else
                {
                  additionalDeploymentConfiguration.setPart(part);
                }
              if(log.isDebugEnabled()) log.debug("adding additional conf read "+node);
            }
        }
      catch (KeeperException|InterruptedException e)
        {
          throw new RuntimeException("deployment", e);
        }
  
      //
      //  (sorry for the 3rd copy/past...) this load file of deployment-product-xxx.json, which can be splited in sub parts
      //
  
      try
      {
        for (String node : zookeeper.getChildren(getZookeeperRoot() + "/deployment", null, null))
        {
          if(log.isDebugEnabled()) log.debug("checking for product conf "+node);
          if (!node.startsWith("product-")) continue;//takes only the product ones
          byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/deployment/" + node, null, null);
          if (bytes == null || bytes.length <= 1) continue;
          DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node.replace("product-",""), bytes, false);
          if (part == null) continue;
          DeploymentConfiguration productDeploymentConfiguration = productDeploymentConfigurations.get(part.getBaseName());
          if (productDeploymentConfiguration == null)
          {
            productDeploymentConfiguration = new DeploymentConfiguration(part);
            productDeploymentConfigurations.put(part.getBaseName(), productDeploymentConfiguration);
          }
          else
          {
            productDeploymentConfiguration.setPart(part);
          }
          if(log.isDebugEnabled()) log.debug("adding product conf read "+node);
        }
      }
      catch (KeeperException|InterruptedException e)
      {
        throw new RuntimeException("deployment", e);
      }
    }

    //
    //  close
    //

    try
      {
        zookeeper.close();
      }
    catch (InterruptedException e)
      {
        // ignore
      }

    /*****************************************
    *
    *  configuration -- json
    *
    *****************************************/

    try
      {

        //
        //  process additional deployment nodes first of "custo" conf
        //

        JSONObject custoJson=new JSONObject();
        for (DeploymentConfiguration additionalDeploymentConfiguration : additionalDeploymentConfigurations.values())
          {
            JSONObject additionalJsonConfiguration = (JSONObject) (new JSONParser()).parse(additionalDeploymentConfiguration.getContents());
            if(log.isDebugEnabled()) log.debug("adding additional conf values of "+additionalDeploymentConfiguration.getBaseName());
            custoJson.putAll(additionalJsonConfiguration);
          }

        //
        //  base deployment node
        //

        JSONObject baseJsonConfiguration = (JSONObject) (new JSONParser()).parse(deploymentConfiguration.getContents());
        if(log.isDebugEnabled()) log.debug("adding base conf values of "+deploymentConfiguration.getBaseName());
        custoJson.putAll(baseJsonConfiguration);

        //
        // now process the "product" ones
        //

        JSONObject productJson=new JSONObject();
        for (DeploymentConfiguration productDeploymentConfiguration : productDeploymentConfigurations.values())
        {
          JSONObject productJsonConfiguration = (JSONObject) (new JSONParser()).parse(productDeploymentConfiguration.getContents());
          if(log.isDebugEnabled()) log.debug("adding product conf values of "+productDeploymentConfiguration.getBaseName());
          productJson.putAll(productJsonConfiguration);
        }

        //
        // merge both
        //
        jsonRoot = JSONUtilities.jsonMergerOverrideOrAdd(productJson,custoJson,(product,custo) -> product.get("id")!=null && custo.get("id")!=null && product.get("id").equals(custo.get("id")));//json object in array match thanks to "id" field only
		// the final running conf could be so hard to understand from all deployment files, we have to provide it to support team, hence the info log, even if big :
		log.info("LOADED CONF : "+jsonRoot.toJSONString());

      }
    catch (org.json.simple.parser.ParseException e)
      {
        throw new RuntimeException("deployment", e);
      }

    /*****************************************
    *
    *  baseTimeZone
    *
    *****************************************/

    try
      {
        baseTimeZone = JSONUtilities.decodeString(jsonRoot, "baseTimeZone", true);
        baseZoneId = ZoneId.of(baseTimeZone);
      }
    catch (JSONUtilitiesException e)
      {
        throw new RuntimeException("deployment", e);
      }

    /*****************************************
    *
    *  baseLanguage
    *
    *****************************************/

    try
      {
        baseLanguage = JSONUtilities.decodeString(jsonRoot, "baseLanguage", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new RuntimeException("deployment", e);
      }

    /*****************************************
    *
    *  baseCountry
    *
    *****************************************/

    try
      {
        baseCountry = JSONUtilities.decodeString(jsonRoot, "baseCountry", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new RuntimeException("deployment", e);
      }

    /*****************************************
    *
    *  subscribermanager configuration
    *
    *****************************************/

    //
    //  alternateIDs
    //

    try
      {
        JSONArray alternateIDValues = JSONUtilities.decodeJSONArray(jsonRoot, "alternateIDs", false);
        if (alternateIDValues != null)
          {
            for (int i=0; i<alternateIDValues.size(); i++)
              {
                JSONObject alternateIDJSON = (JSONObject) alternateIDValues.get(i);
                AlternateID alternateID = new AlternateID(alternateIDJSON);
                alternateIDs.put(alternateID.getID(), alternateID);
              }
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment : alternateIDs", e);
      }

    //
    //  assignSubscriberIDsTopic
    //

    try
      {
        assignSubscriberIDsTopic = JSONUtilities.decodeString(jsonRoot, "assignSubscriberIDsTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : assignSubscriberIDsTopic", e);
      }
    
    //
    //  updateExternalSubscriberIDTopic
    //

    try
      {
        updateExternalSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "updateExternalSubscriberIDTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : updateExternalSubscriberIDTopic", e);
      }

    //
    //  assignExternalSubscriberIDsTopic
    //

    try
      {
        assignExternalSubscriberIDsTopic = JSONUtilities.decodeString(jsonRoot, "assignExternalSubscriberIDsTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : assignExternalSubscriberIDsTopic", e);
      }
    
    //
    //  recordSubscriberIDTopic
    //

    try
      {
        recordSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "recordSubscriberIDTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : recordSubscriberIDTopic", e);
      }
    
    //
    //  recordAlternateIDTopic
    //

    try
      {
        recordAlternateIDTopic = JSONUtilities.decodeString(jsonRoot, "recordAlternateIDTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : recordAlternateIDTopic", e);
      }
    
    //
    //  autoProvisionedSubscriberChangeLog
    //

    try
      {
        autoProvisionedSubscriberChangeLog = JSONUtilities.decodeString(jsonRoot, "autoProvisionedSubscriberChangeLog", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : autoProvisionedSubscriberChangeLog", e);
      }
    
    //
    //  autoProvisionedSubscriberChangeLogTopic
    //

    try
      {
        autoProvisionedSubscriberChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "autoProvisionedSubscriberChangeLogTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : autoProvisionedSubscriberChangeLogTopic", e);
      }
    
    //
    //  rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic
    //

    try
      {
        rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic = JSONUtilities.decodeString(jsonRoot, "rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic", (alternateIDs.size() > 0));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic", e);
      }

    //
    //  cleanupSubscriberTopic
    //

    try
      {
        cleanupSubscriberTopic = JSONUtilities.decodeString(jsonRoot, "cleanupSubscriberTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  externalSubscriberID
    //

    externalSubscriberID = null;
    for (AlternateID alternateID : alternateIDs.values())
      {
        if (alternateID.getExternalSubscriberID())
          {
            if (alternateID.getSharedID()) throw new ServerRuntimeException("externalSubscriberID cannot be specified to be a shared id");
            if (externalSubscriberID != null) throw new ServerRuntimeException("multiple externalSubscriberID alternateIDs");
            externalSubscriberID = alternateID.getID();
          }
      }

    //
    //  subscriberTraceControlAlternateID
    //

    try
      {
        subscriberTraceControlAlternateID = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlAlternateID", false);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : subscriberTraceControlAlternateID", e);
      }

    //
    //  subscriberTraceControlAutoProvision
    //

    try
      {
        subscriberTraceControlAutoProvision = JSONUtilities.decodeBoolean(jsonRoot, "subscriberTraceControlAutoProvision", Boolean.FALSE);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : subscriberTraceControlAutoProvision", e);
      }

    //
    //  subscriberTraceControlTopic
    //

    try
      {
        subscriberTraceControlTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlTopic", "subscribertracecontrol");
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : subscriberTraceControlTopic", e);
      }

    //
    //  subscriberTraceControlAssignSubscriberIDTopic
    //

    try
      {
        subscriberTraceControlAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlAssignSubscriberIDTopic", "subscribertracecontrol-assignsubscriberid");
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : subscriberTraceControlAssignSubscriberIDTopic", e);
      }

    //
    //  subscriberTraceTopic
    //

    try
      {
        subscriberTraceTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceTopic", "subscribertrace");
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : subscriberTraceTopic", e);
      }

    //
    //  simulatedTimeTopic
    //

    try
      {
        simulatedTimeTopic = JSONUtilities.decodeString(jsonRoot, "simulatedTimeTopic", "simulatedtime");
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment : simulatedTimeTopic", e);
      }

    //
    //  autoProvisionEvents
    //

    try
      {
        JSONArray autoProvisionEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "autoProvisionEvents", false);
        if (autoProvisionEventValues != null)
          {
            for (int i=0; i<autoProvisionEventValues.size(); i++)
              {
                JSONObject autoProvisionEventJSON = (JSONObject) autoProvisionEventValues.get(i);
                AutoProvisionEvent autoProvisionEvent = new AutoProvisionEvent(autoProvisionEventJSON);
                autoProvisionEvents.put(autoProvisionEvent.getID(), autoProvisionEvent);
              }
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment : autoProvisionEvents", e);
      }

    //
    //  cleanupSubscriberElasticsearchIndexes
    //

    try
      {
        //
        //  cleanupSubscriberElasticsearchIndexes (evolution)
        //

        JSONArray subscriberESIndexesJSON = JSONUtilities.decodeJSONArray(jsonRoot, "cleanupSubscriberElasticsearchIndexes", new JSONArray());
        for (int i=0; i<subscriberESIndexesJSON.size(); i++)
          {
            cleanupSubscriberElasticsearchIndexes.add((String) subscriberESIndexesJSON.get(i));
          }

        //
        //  deploymentCleanupSubscriberElasticsearchIndexes (deployment)
        //

        JSONArray deploymentSubscriberESIndexesJSON = JSONUtilities.decodeJSONArray(jsonRoot, "deploymentCleanupSubscriberElasticsearchIndexes", new JSONArray());
        for (int i=0; i<deploymentSubscriberESIndexesJSON.size(); i++)
          {
            cleanupSubscriberElasticsearchIndexes.add((String) deploymentSubscriberESIndexesJSON.get(i));
          }            
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    // Elasticsearch
    //
    try
      {
        // Elasticsearch settings
        elasticsearchDateFormat = JSONUtilities.decodeString(jsonRoot, "elasticsearchDateFormat", true);
        elasticsearchScrollSize = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchScrollSize", true);
        
        // Elasticsearch shards & replicas
        elasticsearchDefaultShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchDefaultShards", true);
        elasticsearchDefaultReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchDefaultReplicas", true);
        elasticsearchSubscriberprofileShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSubscriberprofileShards", elasticsearchDefaultShards);
        elasticsearchSubscriberprofileReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSubscriberprofileReplicas",elasticsearchDefaultReplicas);
        elasticsearchSnapshotShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSnapshotShards", elasticsearchDefaultShards);
        elasticsearchSnapshotReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSnapshotReplicas",elasticsearchDefaultReplicas);
        elasticsearchLiveVoucherShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchLiveVoucherShards", elasticsearchDefaultShards);
        elasticsearchLiveVoucherReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchLiveVoucherReplicas",elasticsearchDefaultReplicas);
        
        // Elasticsearch retention days
        elasticsearchRetentionDaysODR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysODR", true);
        elasticsearchRetentionDaysBDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysBDR", true);
        elasticsearchRetentionDaysMDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysMDR", true);
        elasticsearchRetentionDaysTokens = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysTokens", true);
        elasticsearchRetentionDaysSnapshots = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysSnapshots", true);
        elasticsearchRetentionDaysJourneys = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysJourneys", true);
        elasticsearchRetentionDaysCampaigns = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysCampaigns", true);
        elasticsearchRetentionDaysBulkCampaigns = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysBulkCampaigns", true);
        elasticsearchRetentionDaysExpiredVouchers = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysExpiredVouchers", true);
        elasticsearchRetentionDaysVDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysVDR", true);
      }
    catch (JSONUtilitiesException|NumberFormatException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
  };

  /****************************************
  *
  *  DeploymentConfiguration
  *
  ****************************************/

  private static class DeploymentConfiguration
  {
    /****************************************
    *
    *  attributes
    *
    ****************************************/
    
    private String baseName;
    private TreeMap<Integer, DeploymentConfigurationPart> parts = new TreeMap<>();

    /****************************************
    *
    *  accessors
    *
    ****************************************/
    
    public String getBaseName() { return baseName; }

    //
    //  getContents
    //

    public String getContents()
    {
      StringBuilder result = new StringBuilder();
      int expectedPart = 1;
      for (Integer partNumber : parts.keySet())
        {
          if (partNumber.intValue() != expectedPart) throw new RuntimeException("deployment component " + baseName + " missing part " + expectedPart);
          result.append(parts.get(partNumber).getContents());
          expectedPart += 1;
        }
      return result.toString();
    }
    
    /****************************************
    *
    *  setters
    *
    ****************************************/
    
    public void setPart(DeploymentConfigurationPart part)
    {
      if (parts.containsKey(part.getPartNumber())) throw new RuntimeException("part " + part.getPartNumber() + " already exists for deployment component " + baseName);
      if (! Objects.equals(baseName, part.getBaseName())) throw new RuntimeException();
      parts.put(part.getPartNumber(), part);
    }

    /****************************************
    *
    *  constructor
    *
    ****************************************/

    public DeploymentConfiguration(DeploymentConfigurationPart part)
    {
      this.baseName = part.getBaseName();
      parts.put(part.getPartNumber(), part);
    }
  }

  /****************************************
  *
  *  DeploymentConfigurationPart
  *
  ****************************************/

  private static class DeploymentConfigurationPart
  {
    //
    //  attributes
    //
    
    private String baseName;
    private int partNumber;
    private String contents;

    //
    //  accessors
    //

    String getBaseName() { return baseName; }
    int getPartNumber() { return partNumber; }
    String getContents() { return contents; }
    
    //
    //  constructor
    //
    
    private DeploymentConfigurationPart(String baseName, int partNumber, String contents)
    {
      this.baseName = baseName;
      this.partNumber = partNumber;
      this.contents = contents;
    }

    //
    //  static processor
    //

    static DeploymentConfigurationPart process(String fullName, byte[] bytes, boolean deploymentPrefix)
    {
      //
      //  parse name
      //
      
      Pattern pattern = deploymentPrefix ? Pattern.compile("^deployment(-([a-zA-Z0-9]+))?(_part_([0-9]+))?$") : Pattern.compile("^([a-zA-Z0-9]+)?(_part_([0-9]+))?$");
      Matcher matcher = pattern.matcher(fullName);
      
      //
      //  return null if no contents or not matching
      //

      if (bytes == null || bytes.length == 0 || ! matcher.matches())
        {
          return null;
        }
      
      //
      //  parse out name and part number
      //
      
      int baseNameGroup = deploymentPrefix ? 2 : 1;
      int partNumberGroup = deploymentPrefix ? 4 : 3;
      String baseName = (matcher.group(baseNameGroup) != null) ? matcher.group(baseNameGroup) : "deployment";
      int partNumber = (matcher.group(partNumberGroup) != null) ? Integer.parseInt(matcher.group(partNumberGroup)) : 1;
      String contents = new String(bytes, StandardCharsets.UTF_8);
      return new DeploymentConfigurationPart(baseName, partNumber, contents);
    }
  }
}
