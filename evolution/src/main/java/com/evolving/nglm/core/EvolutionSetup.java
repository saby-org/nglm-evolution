/*****************************************************************************
*
*  EvolutionSetup.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchUpgrade;
import com.evolving.nglm.evolution.kafka.Topic;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;

import org.apache.avro.data.Json;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HttpHeaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EvolutionSetup
{
  private static HttpClient httpClient;

  /****************************************
   *
   * main
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   *
   ****************************************/
  public static void main(String[] args) throws InterruptedException, ExecutionException
  {
    try {
      //
      // extracts files from args
      //
      String rootPath = args[0];
      String topicsFolderPath = rootPath; // We will filter it and only process topics-* files.
      String elasticsearchUpdateFilePath = rootPath + "elasticsearch/update";
      String connectorsFilePath = rootPath + "connectors/connectors";
  
      //
      // init utilities
      //
      PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
      httpClientConnectionManager.setDefaultMaxPerRoute(50);
      httpClientConnectionManager.setMaxTotal(150);
      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(httpClientConnectionManager);
      httpClient = httpClientBuilder.build();
      
      //
      // Deployment.json load & check
      //
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("= DEPLOYMENT CONFIGURATION CHECK                                               =");
      System.out.println("================================================================================");
      Deployment.initialize();
  
      //
      // kafka topics
      //
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("= KAFKA                                                                        =");
      System.out.println("================================================================================");
      handleTopicSetup(topicsFolderPath);
  
      //
      // elasticSearch index setup
      //
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("= ELASTICSEARCH                                                                =");
      System.out.println("================================================================================");
      handleElasticsearchUpdate(elasticsearchUpdateFilePath);

      //
      // elasticSearch index upgrade - check for each indexes their template version and try to upgrade them if needed
      //
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("= ELASTICSEARCH INDEXES UPGRADE (BETA)                                         =");
      System.out.println("================================================================================");
      handleElasticsearchUpgrade(elasticsearchUpdateFilePath);
  
      //
      // kafka connect setup (must be last, after topics & indexes setup)
      //
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("= KAFKA CONNECTORS                                                             =");
      System.out.println("================================================================================");
      handleConnectors(connectorsFilePath);
    }
    catch (Throwable e) {
      System.out.println("[ERROR]: " + e.getMessage());
      e.printStackTrace(System.out);
      System.out.println("");
      System.out.println("================================================================================");
      System.out.println("Setup FAILED"); // Do not change this print or you need to change the check in core-deploy-setup.sh & core-upgrade-setup.sh !
      System.exit(-1);
    }
  }

  /****************************************
   *
   * handleElasticsearch
   *
   ****************************************/

  private static void handleElasticsearchUpdate(String elasticsearchUpdateFilePath) throws ParseException, EvolutionSetupException {
    List<CurlCommand> curls = handleCurlFile(elasticsearchUpdateFilePath);
    for(CurlCommand cmd : curls) {
      ObjectHolder<String> responseBody = new ObjectHolder<String>();
      ObjectHolder<Integer> httpResponseCode = new ObjectHolder<Integer>();
      
      //
      // First check if the item exist
      //
      int retry = 10;
      while(httpResponseCode.getValue() == null || httpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
        if(retry < 0) { 
          throw new EvolutionSetupException("Fail to get an answer after retrying several times: " + responseBody.getValue());
        }
        
        executeCurl(cmd.url, "{}", "-XGET", cmd.username, cmd.password, httpResponseCode, responseBody);
        retry--;
      }

      // 
      // Retrieve the previous version, if any and update command URL
      //
      String updatedURL = cmd.url;
      if(httpResponseCode.getValue() == 200) {
        System.out.println("[DISPLAY] INFO: There is a previous version in the system.");
        
        // There is a special case for ISM objects. Update function need to retrieve current version
        if(updatedURL.contains("/_ism/")) {
          JSONObject answer = (JSONObject) (new JSONParser()).parse(responseBody.getValue());
          updatedURL += "?if_seq_no="+answer.get("_seq_no")+"&if_primary_term="+answer.get("_primary_term");
        }
      } else if(httpResponseCode.getValue() == 404) {
        System.out.println("[DISPLAY] INFO: Item does not exist yet.");
      } else {
        throw new EvolutionSetupException("Unknown response code (" + httpResponseCode.getValue() + "): " + responseBody.getValue());
      }
      
      responseBody.setValue(null);
      httpResponseCode.setValue(null);
      retry = 10;
      while(httpResponseCode.getValue() == null || httpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
        if(retry < 0) { 
          throw new EvolutionSetupException("Fail to get an answer after retrying several times: " + responseBody.getValue());
        }
        
        executeCurl(updatedURL, cmd.jsonBody, cmd.verb, cmd.username, cmd.password, httpResponseCode, responseBody);
        retry--;
      }

      if (! (httpResponseCode.getValue().intValue() >= 200 && httpResponseCode.getValue().intValue() < 300)) {
        throw new EvolutionSetupException("Unable to update Elasticsearch on " + cmd.url + ". " + responseBody.getValue());
      } else {
        System.out.println("[DISPLAY] INFO: Item has been updated.");
      }
    }
  }

  /****************************************
   *
   * handleElasticsearch
   *
   ****************************************/

  private static void handleElasticsearchUpgrade(String elasticsearchUpdateFilePath) throws ParseException, EvolutionSetupException{
    //
    // Retrieve master root info
    //
    List<CurlCommand> curls = handleCurlFile(elasticsearchUpdateFilePath);
    CurlCommand templateCmd = null;
    for(CurlCommand cmd : curls) {
      if(cmd.url.endsWith("_template/root")) {
        templateCmd = new CurlCommand(cmd.verb, cmd.url.replace("/_template/root", "/"), cmd.username, cmd.password, cmd.jsonBody);
        break;
      }
    }
    if(templateCmd == null) {
      throw new EvolutionSetupException("Unable to retrieve master info.");
    }
    
    //
    // Retrieve indexes list (GET http://HOST:PORT/*)
    //
    List<String> indexes = new LinkedList<String>();
    ObjectHolder<String> responseBody = new ObjectHolder<String>();
    ObjectHolder<Integer> httpResponseCode = new ObjectHolder<Integer>();
    JSONObject answer;
    executeCurl(templateCmd.url + "*", "{}", "-XGET", templateCmd.username, templateCmd.password, httpResponseCode, responseBody);
    if(httpResponseCode.getValue() == 200) {
      answer = (JSONObject) (new JSONParser()).parse(responseBody.getValue());
    } 
    else {
      throw new EvolutionSetupException("Unable to retrieve Elasticsearch index list: "+ responseBody.getValue());
    }
    
    for(Object key: answer.keySet()) {
      indexes.add((String) key);
    }
    
    //
    // Retrieve indexes _template version for every index
    //
    Map<String,Long> templatesVersions = Deployment.getElasticsearchTemplatesVersion();
    Map<String, Map<String,Long>> indexesVersions = new LinkedHashMap<String, Map<String,Long>>();
    for(String index: indexes) {
      responseBody = new ObjectHolder<String>();
      httpResponseCode = new ObjectHolder<Integer>();
      answer = null;
      Map<String,Long> versions = new LinkedHashMap<String,Long>();
      executeCurl(templateCmd.url + index, "{}", "-XGET", templateCmd.username, templateCmd.password, httpResponseCode, responseBody);
      if(httpResponseCode.getValue() == 200) {
        answer = (JSONObject) (new JSONParser()).parse(responseBody.getValue());
        try {
          JSONObject info = (JSONObject) answer.get(index);
          JSONObject mappings = (JSONObject) info.get("mappings");
          JSONObject meta = (JSONObject) mappings.get("_meta");
          for(Object template : meta.keySet()) {
            if(templatesVersions.get(template) == null) {
              continue;
            }
            Long version = (Long) ((JSONObject) meta.get(template)).get("version");
            if(version != null) {
              versions.put((String) template, version);
            }
          }
        }
        catch (Exception e) {
          // Unable to retrieve version, check if upgrade from Evolution version < 2.0.0
          String template = ElasticsearchUpgrade.recoverTemplateVersion0(index);
          if(template != null) {
            versions.put(template, 0L); // before Evolution 2.0.0 : set template version to 0
          }
          else {
            // If index is not referenced, discard it 
            continue;
          }
        }
        
        indexesVersions.put(index, versions);
      } 
      else {
        throw new EvolutionSetupException("Unable to retrieve mapping from "+index+" ES index. "+ responseBody.getValue());
      }
    }
    
    //
    // Check all _template version
    //
    boolean allOk = true;
    List<String> upgradeNeeded = new LinkedList<String>();
    for(String index: indexesVersions.keySet()) {
      Map<String,Long> versions = indexesVersions.get(index);
      for(String template: versions.keySet()) {
        Long version = versions.get(template);
        if(templatesVersions.get(template) != version) {
          upgradeNeeded.add(index);
          allOk = false;
        }
        System.out.println("[DISPLAY] Checking '"+template+"' template for index ["+index+"] (current: "+version+", expected: "+templatesVersions.get(template)+").");
      }
    }
    if(!allOk) {
      if(Deployment.getElasticsearchTemplateVersionFailOnCheck()) {
        throw new EvolutionSetupException("Your system require some manual updates (found some Elasticsearch indexes in deprecated version).");
      }
      else {
        System.out.println("[WARNING]: Your system require some manual updates (found some Elasticsearch indexes in deprecated version).");
        System.out.println("[WARNING]: Template version check failed, but fail-on-check is disable, deployment will continue.");
      }
    }
  }

  /****************************************
   *
   * Kafka Topics
   *
   ****************************************/

  private static void getFinalTopicSetup(String topicSetupFileName, Map<String, NewTopic> topicsToSetup) throws EvolutionSetupException
  {
    String line = null;
    BufferedReader reader = null;
    try
      {
        reader = new BufferedReader(new FileReader(new File(topicSetupFileName)));
        Pattern mainPattern = Pattern.compile("^create_topic\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.*)$");
        Pattern optionPattern = Pattern.compile("\\s*(\\S+)=(\\S+)");
        while (true)
          {
            //
            // get line
            //

            line = reader.readLine();
            if (line == null) break;

            //
            // parse line
            // create_topic <topicName> <partitions> <replicationFactor> --config
            // param1=value1 --config param2=value2
            //

            Matcher mainMatcher = mainPattern.matcher(line);
            if (mainMatcher.matches())
              {
                //
                // required parameters
                // - topicName
                // - replicationFactor
                // - partitions
                //

                String topicName = mainMatcher.group(1);
                short replicationFactor = Short.parseShort(mainMatcher.group(2));
                int partitions = Integer.parseInt(mainMatcher.group(3));

                //
                // additional configuration
                //

                String additionalParameters = mainMatcher.group(4);
                Map<String, String> configs = new HashMap<>();
                Matcher optionMatcher = optionPattern.matcher(additionalParameters);
                while (optionMatcher.find())
                  {
                    configs.put(optionMatcher.group(1), optionMatcher.group(2));
                  }

                //
                // new topic
                //

                NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor).configs(configs);
                topicsToSetup.put(topicName, newTopic);
              }
            else
              {
                System.out.println("[DISPLAY] WARNING: skipping badly-formatted topic configuration line: '" + line + "'");
              }
          }
      }
    catch (IOException e)
      {
        throw new EvolutionSetupException("Problems reading topic configuration : " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
      }
    finally
      {
        if (reader != null) try
          {
            reader.close();
          }
        catch (IOException e)
          {
            System.out.println("[DISPLAY] WARNING: Problem while closing the reader " + topicSetupFileName);
          }
      }
  }

  private static void handleTopicSetup(String topicsFolderPath) throws EvolutionSetupException
  {
    //
    // get topics that need to exist
    //

    Map<String, NewTopic> topicsToSetup = new HashMap<>();

    // start by auto create ones (then would be override by file if specified in file)
    Deployment.getAllTopics().forEach(topic->topicsToSetup.put(topic.getName(),topic.getNewTopic()));

    File setupRep = new File(topicsFolderPath);
    for(File current : setupRep.listFiles()) {
      if(current.getName().startsWith("topics-")) {
        getFinalTopicSetup(current.getAbsolutePath(), topicsToSetup);
      }
    }

    //
    // get topics that already exists
    //
    Map<String, NewTopic> existingTopics = getExisitngTopics();

    //
    // compare topics to be created with topics already created
    //

    Map<String, NewTopic> topicsToCreate = new HashMap<>();
    for (NewTopic topicToSetup : topicsToSetup.values())
      {
        NewTopic existingTopic = existingTopics.get(topicToSetup.name());
        if (existingTopic == null)
          {
            topicsToCreate.put(topicToSetup.name(), topicToSetup);
          }
        else
          {

            //
            // compare the configurations of the topic
            //

            boolean same = true;

            if (topicToSetup.numPartitions() != existingTopic.numPartitions())
              {
                same = false;
              }
            if (topicToSetup.replicationFactor() != existingTopic.replicationFactor())
              {
                same = false;
              }
            for (Map.Entry<String, String> prop : topicToSetup.configs().entrySet())
              {
                String existingProperties = existingTopic.configs().get(prop.getKey());
                if (existingProperties == null)
                  {
                    same = false;
                  }
                else if (!existingProperties.equals(prop.getValue()))
                  {
                    same = false;
                  }
              }

            if (same == false)
              {
                System.out.println("[DISPLAY] MANDATORY: Topic " + topicToSetup.name() + " must be updated from " + existingTopic.toString() + " to " + topicToSetup);
              }
          }
      }

    //
    // Create missing topics
    //

    Properties adminClientConfig = new Properties();
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("broker.servers"));

    AdminClient adminClient = AdminClient.create(adminClientConfig);

    for (NewTopic topicToCreate : topicsToCreate.values())
      {
        try
          {
            System.out.println("[DISPLAY] INFO: Create Topic " + topicToCreate);
            createSingleTopic(adminClient, topicToCreate);
          }
        catch (InterruptedException | ExecutionException e)
          {
            System.out.println("[DISPLAY] WARNING: problems creating topic '" + topicToCreate.name() + "': " + e.getMessage());
          }
      }
  }
  
  /*
   * Public method to create a topic.
   */
  
  public static void createSingleTopic(AdminClient adminClient, NewTopic topicToCreate) throws InterruptedException, ExecutionException
  {
    CreateTopicsResult result = adminClient.createTopics(Collections.singleton(topicToCreate));
    for (KafkaFuture<Void> future : result.values().values())
      future.get();
  }

  private static Map<String, NewTopic> getExisitngTopics()
  {

    Map<String, NewTopic> existingTopics = new HashMap<>();

    //
    // kafka topics setup
    //

    KafkaZkClient zkClient = KafkaZkClient.apply(System.getProperty("zookeeper.connect"), false, 10000, 100000, 30, Time.SYSTEM, "foo", "bar", null);
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);

    //
    // handle properties from Zookeeper: cleanup.policy segment.bytes
    // min.compaction.lag.ms delete.retention.ms retention.ms
    //

    Map<String, Properties> kafkaZKTopics = scala.collection.JavaConverters.mapAsJavaMapConverter(adminZkClient.getAllTopicConfigs()).asJava();
    for (Map.Entry<String, Properties> kafkaZKTopic : kafkaZKTopics.entrySet())
      {
        Properties properties = kafkaZKTopics.get(kafkaZKTopic.getKey());
        String topicName = kafkaZKTopic.getKey();
        NewTopic existingTopic = new NewTopic(topicName, 1 /* Not known yet */, (short) 1 /* Not known yet */);
        existingTopics.put(topicName, existingTopic);
        Map<String, String> configs = new HashMap<>();
        existingTopic.configs(configs);
        for (Map.Entry<Object, Object> prop : properties.entrySet())
          {
            configs.put((String) (prop.getKey()), (String) (prop.getValue()));
          }
      }

    //
    // handle properties from Kafka: partitions replicationFactor
    //

    Properties adminClientConfig = new Properties();
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("broker.servers"));
    AdminClient adminClient = AdminClient.create(adminClientConfig);

    ListTopicsResult topicsResult = adminClient.listTopics();
    KafkaFuture<Collection<TopicListing>> future = topicsResult.listings();
    try
      {
        Collection<TopicListing> topics = future.get();
        ArrayList<String> topicNames = new ArrayList<>();
        for (TopicListing topic : topics)
          {
            topicNames.add(topic.name());
          }

        DescribeTopicsResult describeTopicResult = adminClient.describeTopics(topicNames);
        for (KafkaFuture<TopicDescription> topicDescriptionFuture : describeTopicResult.values().values())
          {
            TopicDescription topicDescription = topicDescriptionFuture.get();
            List<TopicPartitionInfo> partitions = topicDescription.partitions();
            int partitionNumber = partitions.size();
            short replicationFactor = 0;
            for (TopicPartitionInfo partition : partitions)
              {
                replicationFactor = partition.replicas().size() > replicationFactor ? (short) (partition.replicas().size()) : replicationFactor;
              }

            //
            // complete map from zookeeper for each topic
            //

            NewTopic topic = existingTopics.get(topicDescription.name());
            if (topic == null)
              {
                System.out.println("[DISPLAY] WARNING: Topic " + topicDescription.name() + " not retrieved from Kafka");
              }
            else
              {
                NewTopic updatedTopic = new NewTopic(topicDescription.name(), partitionNumber, replicationFactor);
                updatedTopic.configs(topic.configs());
                existingTopics.put(topicDescription.name(), updatedTopic);
              }
          }
        return existingTopics;
      }
    catch (InterruptedException | ExecutionException e)
      {
        System.out.println("[DISPLAY] WARNING: problems getting topic configuration " + e.getMessage());
      }
    return null;
  }

  /****************************************
   *
   * Kafka Connectors
   *
   ****************************************/

  private static void handleConnectors(String connectorsFilePath) throws EvolutionSetupException
  {
    HashMap<String,Set<String>> allFromConf = new HashMap<>();//this will register <url,connectorsName> from conf file, to list what might need to be deleted at the end
    BufferedReader reader = null;
    String line = null;
    try
      {

        reader = new BufferedReader(new FileReader(new File(connectorsFilePath)));
        while (true)
          {
            //
            // get line
            //

            line = reader.readLine();
            if (line == null) break;

            if (line.trim().equals(""))
              {
                continue;
              }

            //
            // extract all parameters from the line
            //

            System.out.println("DEBUG: Handle connector line " + line);
            
            String[] params = line.split("\\|\\|\\|");

            //
            // retrieve the connector's configuration JSON
            //
            String jsonConnectorToSetupConfig = null;
            String url = null;
            for (int i = 0; i < params.length; i++)
              {
                if (params[i].trim().equals("-d"))
                  {
                    jsonConnectorToSetupConfig = params[i + 2]; // because i+1 refers to a space separator " "
                  }
                if (params[i].trim().startsWith("http"))
                  {
                    url = params[i];
                  }
              }

            try
              {
                JSONObject connectorToSetup = (JSONObject) (new JSONParser()).parse(jsonConnectorToSetupConfig);
                String connectorName = com.evolving.nglm.core.JSONUtilities.decodeString(connectorToSetup, "name");
                Set<String> fromConf = allFromConf.get(url)!=null?allFromConf.get(url):new HashSet<>();
                fromConf.add(connectorName);
                allFromConf.put(url,fromConf);

                // connectors on dynamic topics special case (dirty, no real easy way..., the quickest I found)
                List<String> toAdd = new ArrayList<>();
                if(connectorName.equals("notification_es_sink_connector")){
                  for(CommunicationChannel cc:Deployment.getCommunicationChannels().values()){
                    if(cc.getDeliveryManagerDeclaration()!=null){
                      // this is for generic communication channels in fact...
                      for(Topic topic:cc.getDeliveryManagerDeclaration().getResponseTopics()){
                        toAdd.add(topic.getName());
                        System.out.println("Prepare to add topic for notification_es_sink_connector: " + topic.getName());
                      }
                    }
                    else
                      {
                        // for old channels // TODO remove asap // let retrieve the configuration through deliveryType
                        DeliveryManagerDeclaration dmd = Deployment.getDeliveryManagers().get(cc.getDeliveryType());
                        if(dmd != null)
                          {
                            for(Topic topic:dmd.getResponseTopics()){
                              toAdd.add(topic.getName());
                              System.out.println("Prepare to add topic for old channels notification_es_sink_connector: " + topic.getName());
                            }
                          }
                        else
                          {
                            System.out.println("notification_es_sink_connector Don't retrieve deliveryManager config for " + cc.getDeliveryType());
                          }
                      }
                  }
                }else if(connectorName.equals("bdr_es_sink_connector")){
                  for(DeliveryManagerDeclaration deliveryManagerDeclaration:Deployment.getDeliveryManagers().values()){
                    if(!deliveryManagerDeclaration.logBDR()) continue;//not to log BDR
                    for(Topic topic:deliveryManagerDeclaration.getResponseTopics()){
                      toAdd.add(topic.getName());
                    }
                  }
                }else if(connectorName.equals("odr_es_sink_connector")){
                  DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get(PurchaseFulfillmentManager.PURCHASEFULFILLMENT_DELIVERY_TYPE);
                  if(deliveryManagerDeclaration!=null){
                    for(Topic topic:deliveryManagerDeclaration.getResponseTopics()){
                      toAdd.add(topic.getName());
                    }
                  }
                }
                // need to put some dynamic topic ?
                if(!toAdd.isEmpty()){
                  JSONObject config = (JSONObject)connectorToSetup.get("config");
                  String topicToAdd = String.join(",",toAdd);
                  String topicsConf = (String)config.get("topics");
                  if(topicsConf==null){
                    config.put("topics",topicToAdd);
                  }else{
                    topicsConf=topicsConf+","+topicToAdd;
                    config.put("topics",topicsConf);
                    System.out.println("[DISPLAY] INFO: Connector " + connectorName + " seems to be a mixed on provided static conf and dynamic one, final topics result : "+topicsConf);
                  }
                }

                ObjectHolder<String> existingConfigResponseContent = new ObjectHolder<String>();
                ObjectHolder<Integer> existingConfigHttpResponseCode = new ObjectHolder<Integer>();
                boolean alreadyExist = false;
                try
                  {
                    while(existingConfigHttpResponseCode.getValue() == null || existingConfigHttpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
                      executeCurl(url + "/" + connectorName, null, "-XGET", null, null, existingConfigHttpResponseCode, existingConfigResponseContent);
                    }
                    if (existingConfigHttpResponseCode.getValue().intValue() >= 200 && existingConfigHttpResponseCode.getValue().intValue() < 300)
                      {
                        alreadyExist = true;
                      }
                    else if (existingConfigHttpResponseCode.getValue().intValue() == 404)
                      {
                        System.out.println("[DISPLAY] INFO: Connector " + connectorName + " not found so will be created");
                        alreadyExist = false;
                      }
                    else
                      {
                        System.out.println("[DISPLAY] WARNING: could not get configuration of connector " + connectorName + " due HTTP error code " + existingConfigHttpResponseCode.getValue().intValue());
                        continue;
                      }
                  }
                catch (EvolutionSetupException e)
                  {
                    System.out.println("[DISPLAY] WARNING: could not get configuration of connector " + connectorName + " due to exception " + e.getClass().getName());
                    e.printStackTrace();
                    continue;
                  }
                if (alreadyExist)
                  {
                    boolean identical = true;
                    JSONObject existingConnectorConfig = (JSONObject) (new JSONParser()).parse(existingConfigResponseContent.getValue());
                    for (Object propKeyObject : connectorToSetup.keySet())
                      {
                        String propKeyString = (String) propKeyObject;
                        if (propKeyString.equals("config"))
                          {
                            //
                            // iterate over configs in both ways (toSetup vs existing and existing vs setup)
                            //
                            JSONObject configToSetup = com.evolving.nglm.core.JSONUtilities.decodeJSONObject(connectorToSetup, "config");
                            JSONObject existingConfig = com.evolving.nglm.core.JSONUtilities.decodeJSONObject(existingConnectorConfig, "config");
                            for (Object configKeyObject : configToSetup.keySet())
                              {
                                if (existingConfig.get(configKeyObject) == null || !existingConfig.get(configKeyObject).toString().equals(configToSetup.get(configKeyObject).toString()))
                                  {
                                    identical = false;
                                  }
                              }
                            for (Object configKeyObject : existingConfig.keySet())
                              {
                                if (configKeyObject.equals("name"))
                                  {
                                    if (!existingConfig.get(configKeyObject).equals(connectorName))
                                      {
                                        identical = false;
                                        System.out.println("WARNING: name of connector is not coherent existing " + existingConfig.get(configKeyObject) + " to setup " + connectorName);
                                      }
                                  }
                                else if (configToSetup.get(configKeyObject) == null || !configToSetup.get(configKeyObject).toString().equals(existingConfig.get(configKeyObject).toString()))
                                  {
                                    identical = false;
                                  }
                              }
                          }
                      }
                    if (identical == false)
                      {
                        System.out.println("[DISPLAY] INFO: updating connector config from " + existingConfigResponseContent.getValue() + " to config " + connectorToSetup);

                        //
                        // update existing connector with new configuration
                        //
                        try
                          {
                            ObjectHolder<String> updateResponseContent = new ObjectHolder<String>();
                            ObjectHolder<Integer> updateResponseCode = new ObjectHolder<Integer>();
                            executeCurl(url + "/" + connectorName + "/config", JSONUtilities.decodeJSONObject(connectorToSetup, "config").toString(), "-XPUT", null, null, updateResponseCode, updateResponseContent);
                            if (updateResponseCode.getValue().intValue() >= 200 && updateResponseCode.getValue().intValue() < 300)
                              {
                                System.out.println("INFO: upgrade of connector " + connectorName + " well executed");
                              }
                            else
                              {
                                System.out.println("WARNING: Problem while updating connector " + connectorName + " response code " + updateResponseCode.getValue().intValue());
                              }
                          }
                        catch (EvolutionSetupException e)
                          {
                            System.out.println("WARNING: Problem while updating connector " + connectorName + " due to Exception " + e.getMessage());
                            e.printStackTrace();
                          }
                      }
                    else
                      {
                        System.out.println("DEBUG: connector " + connectorName + " not to be updated");
                      }

                  }
                else
                  {
                    //
                    // Does not already exist, create the connector
                    //
                    System.out.println("[DISPLAY] INFO: Connector " + connectorName + " does not exist, ready to create it");
                    ObjectHolder<String> createResponseContent = new ObjectHolder<String>();
                    ObjectHolder<Integer> createResponseCode = new ObjectHolder<Integer>();
                    try
                      {
                        executeCurl(url + "/" + connectorName + "/config", com.evolving.nglm.core.JSONUtilities.decodeJSONObject(connectorToSetup, "config").toString(), "-XPUT", null, null, createResponseCode, createResponseContent);
                      }
                    catch (EvolutionSetupException e)
                      {
                        System.out.println("[DISPLAY] WARNING: Problem while creating connector " + connectorName + " due to Exception " + e.getMessage());
                        e.printStackTrace();
                      }
                  }
              }
            catch (ParseException e)
              {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }

          }

          // log some to delete ?
          for(Map.Entry<String,Set<String>> entry:allFromConf.entrySet()){
            String url = entry.getKey();
            Set<String> connectors = entry.getValue();
            ObjectHolder<String> existingConnectorsResponseContent = new ObjectHolder<String>();
            ObjectHolder<Integer> existingConnectorsHttpResponseCode = new ObjectHolder<Integer>();
            try{
              while(existingConnectorsHttpResponseCode.getValue() == null || existingConnectorsHttpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
                executeCurl(url, null, "-XGET", null, null, existingConnectorsHttpResponseCode, existingConnectorsResponseContent);
              }
              if (existingConnectorsHttpResponseCode.getValue().intValue() == 200){
                Object[] existingConnectors = ((JSONArray)(new JSONParser()).parse(existingConnectorsResponseContent.getValue())).toArray();
                for(Object existingConnector:existingConnectors){
                  if(!connectors.contains(existingConnector)){
                    System.out.println("[DISPLAY] INFO: need to delete connector \""+existingConnector+"\" from "+url);
                  }
                }
              }else{
                System.out.println("[DISPLAY] WARN: could not check connectors to delete "+existingConnectorsHttpResponseCode.getValue().intValue());
              }
            }catch (Exception e){
              System.out.println("[DISPLAY] WARNING: Problem while getting connectors list to " + url + " due to Exception " + e.getMessage());
              e.printStackTrace();
            }
          }

      }
    catch (IOException e)
      {
        throw new EvolutionSetupException("Problems creating connectors: " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
      }
    finally
      {
        if (reader != null) try
          {
            reader.close();
          }
        catch (IOException e)
          {
            System.out.println("[DISPLAY] WARNING: Problem while closing the reader " + connectorsFilePath);
          }
      }
  }

  /****************************************
   *
   * Utils
   *
   ****************************************/

  private static void executeCurl(String url, String jsonRequestEntity, String httpMethod, String username, String password, ObjectHolder<Integer> httpResponseCode, ObjectHolder<String> responseBody) throws EvolutionSetupException
  {
    System.out.println("[DISPLAY] INFO: Executing CURL " + httpMethod + " on " + url);
    HttpResponse httpResponse = null;
    HttpRequestBase httpRequest;
    switch (httpMethod)
      {
      case "-XGET":
        httpRequest = new HttpGet(url);
        break;
      case "-XPOST":
        httpRequest = new HttpPost(url);
        break;
      case "-XDELETE":
        httpRequest = new HttpDelete(url);
        break;
      case "-XPUT":
        httpRequest = new HttpPut(url);
        break;
      default:
        httpRequest = new HttpGet(url); // default
        break;
      }

    if (httpRequest instanceof HttpPost || httpRequest instanceof HttpPut)
      {
        ((HttpEntityEnclosingRequestBase) httpRequest).setEntity(new StringEntity(jsonRequestEntity, ContentType.create("application/json")));
      }

    httpRequest.setConfig(RequestConfig.custom().setConnectTimeout(120).build());    
    if(username != null) {
      String auth = username + ":" + password;
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
      String authHeader = "Basic " + new String(encodedAuth);
      httpRequest.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }
    
    try
      {
        httpResponse = httpClient.execute(httpRequest);
      }
    catch (IOException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        throw new EvolutionSetupException("Exception processing REST api: {}" + stackTraceWriter.toString());
      }
    httpResponseCode.setValue(httpResponse.getStatusLine().getStatusCode());
    if (httpResponse.getEntity() != null)
      {
        try
          {
            responseBody.setValue(EntityUtils.toString(httpResponse.getEntity()));
          }
        catch (IOException e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            throw new EvolutionSetupException("Exception while decoding http response body " + stackTraceWriter.toString());
          }
      }
  }

  private static class ObjectHolder<T>
  {
    private T value;

    public ObjectHolder()
      {

      }

    public T getValue()
    {
      return value;
    }

    public void setValue(T value)
    {
      this.value = value;
    }
  }

  private static class CurlCommand
  {
    public String verb;
    public String url;
    public String jsonBody;
    public String username;
    public String password;

    public CurlCommand(String verb, String url, String username, String password, String jsonBody) {
      this.verb = verb;
      this.url = url;
      this.jsonBody = jsonBody;
      this.username = username;
      this.password = password;
    }
  }

  private static List<CurlCommand> handleCurlFile(String curlFilePath) throws EvolutionSetupException
  {
    BufferedReader reader = null;
    String line = null;
    List<CurlCommand> result = new ArrayList<CurlCommand>();
    try
      {
        reader = new BufferedReader(new FileReader(new File(curlFilePath)));

        line = reader.readLine();
        while (line != null) {
          if (line.trim().equals("")) {
            continue;
          }

          System.out.println("DEBUG: Handle CURL line " + line); // warning: not prod friendly, log contains password
          String[] params = line.split("\\|\\|\\|");

          String verb = null;
          String url = null;
          String jsonBody = null;
          String username = null;
          String password = null;
          for (int i = 0; i < params.length; i++) {
            if (params[i].trim().equals("-d")) {
              jsonBody = params[i + 2]; // because i+1 refers to a space separator " "
              i = i + 2; // forward
            }
            else if (params[i].trim().startsWith("-d")) { // when there is no space between -d and the argument
              jsonBody = params[i].substring(2).trim();
            }
            else if (params[i].trim().equals("-u")) {
              String credentials =  params[i + 2]; // because i+1 refers to a space separator " "
              String[] split = credentials.split(":", 2);
              if(split.length == 2) {
                username = split[0];
                password = split[1];
              } else {
                throw new EvolutionSetupException("Bad format for authentication credentials in CURL command.");
              }
              i = i + 2; // forward
            }
            else if (params[i].trim().startsWith("http")) {
              url = params[i];
            }
            else if (params[i].trim().startsWith("-X")) {
              verb = params[i];
            }
          }

          System.out.println("DEBUG: VERB="+verb+" URL="+url+" USERNAME="+username+" PASSWORD="+password+" BODY="+jsonBody); // warning: not prod friendly, log contains password
          if (verb == null || url == null || jsonBody == null) {
            System.out.println("[DISPLAY] WARNING: Unable to handle CURL line correctly: " + line);
          } else {
            //
            // parse Deployment calls
            //
            jsonBody = parseJsonBody(jsonBody);

            result.add(new CurlCommand(verb, url, username, password, jsonBody));
          }

          line = reader.readLine();
        }
      }
    catch (IOException e)
      {
        throw new EvolutionSetupException("Problems in reading CURL line: " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
      }
    finally
      {
        if (reader != null) try
          {
            reader.close();
          }
        catch (IOException e)
          {
            System.out.println("[DISPLAY] WARNING: Problem while closing the reader " + curlFilePath);
          }
      }

    return result;
  }

  private static String parseJsonBody(String jsonBody) throws EvolutionSetupException
  {
    Pattern deploymentGetterCall = Pattern.compile("Deployment\\.(\\w*)\\(\\)");
    Matcher matcher = deploymentGetterCall.matcher(jsonBody);
    String result = jsonBody;

    Set<String> calls = new HashSet<String>(); // We use a set to keep only one occurrence for each call.
    while(matcher.find()) {
      calls.add(matcher.group(1)); // First parenthesis catch of regex
    }

    for(String call : calls) {
      try
        {
          java.lang.reflect.Method getter = com.evolving.nglm.core.Deployment.class.getMethod(call);
          String replace = getter.invoke(null).toString();
          result = result.replaceAll(Pattern.compile("Deployment\\."+call+"\\(\\)").pattern(), replace);
        }
      catch(InvocationTargetException | NoSuchMethodException| IllegalAccessException| NullPointerException e)
        {
          System.out.println(e.getMessage());
          e.printStackTrace(System.out);
          throw new EvolutionSetupException("Unable to call Deployment." + call);
        }
    }

    System.out.println("DEBUG: JSONBODY="+ result);

    return result;
  }

}
