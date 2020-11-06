/*****************************************************************************
*
*  EvolutionSetup.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;

import org.apache.avro.data.Json;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    //
    // extracts files from args
    //
    String rootPath = args[0];
    String topicsFolderPath = rootPath; // We will filter it and only process topics-* files.
    String elasticsearchCreateFilePath = rootPath + "elasticsearch/create";
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
    // kafka topics
    //
    handleTopicSetup(topicsFolderPath);
    
    //
    // elasticSearch index setup
    //
    handleElasticsearchUpdate(elasticsearchUpdateFilePath); // Override
    handleElasticsearchCreate(elasticsearchCreateFilePath); // New things to push, check if already there

    //
    // kafka connect setup (must be last, after topics & indexes setup)
    //
    handleConnectors(connectorsFilePath);

  }

  /****************************************
   *
   * handleElasticsearch
   *
   ****************************************/

  private static void handleElasticsearchCreate(String elasticsearchCreateFilePath) {
    List<CurlCommand> curls = handleCurlFile(elasticsearchCreateFilePath);
    for(CurlCommand cmd : curls) {
      try
        {
          ObjectHolder<String> responseBody = new ObjectHolder<String>();
          ObjectHolder<Integer> httpResponseCode = new ObjectHolder<Integer>();
          
          //
          // First check if the item exist
          //
          while(httpResponseCode.getValue() == null || httpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
            executeCurl(cmd.url, "{}", "-XGET", httpResponseCode, responseBody);
          }

          JSONObject answer = (JSONObject) (new JSONParser()).parse(responseBody.getValue());
          if(answer.get("found") == null || !(boolean) answer.get("found")) { // can also be null
            responseBody.setValue(null);
            httpResponseCode.setValue(null);
            
            //
            // Not found in ES, push it
            //
            while(httpResponseCode.getValue() == null || httpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
              executeCurl(cmd.url, cmd.jsonBody, cmd.verb, httpResponseCode, responseBody);
            }
            
            if(! (httpResponseCode.getValue().intValue() >= 200 && httpResponseCode.getValue().intValue() < 300)) {
              System.out.println("[DISPLAY] WARNING: Unable to populate Elasticsearch on " + cmd.url);
            }
          }
        } 
      catch (ParseException|EvolutionSetupException e)
        {
          System.out.println("[DISPLAY] ERROR: Something wrong happened while executing curl command. " + e.getMessage());
        }
    }
  }
  
  private static void handleElasticsearchUpdate(String elasticsearchUpdateFilePath) {
    List<CurlCommand> curls = handleCurlFile(elasticsearchUpdateFilePath);
    for(CurlCommand cmd : curls) {
      try
        {
          ObjectHolder<String> responseBody = new ObjectHolder<String>();
          ObjectHolder<Integer> httpResponseCode = new ObjectHolder<Integer>();
          
          while(httpResponseCode.getValue() == null || httpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
            executeCurl(cmd.url, cmd.jsonBody, cmd.verb, httpResponseCode, responseBody);
          }
          
          if (! (httpResponseCode.getValue().intValue() >= 200 && httpResponseCode.getValue().intValue() < 300)) {
            System.out.println("[DISPLAY] WARNING: Unable to update Elasticsearch on " + cmd.url + ". " + responseBody.getValue());
          }
        } 
      catch (EvolutionSetupException e)
        {
          System.out.println("[DISPLAY] ERROR: Something wrong happened while executing curl command. " + e.getMessage());
        }
    }
  }
  
  /****************************************
   *
   * Kafka Topics 
   *
   ****************************************/

  private static void getFinalTopicSetup(String topicSetupFileName, Map<String, NewTopic> topicsToSetup)
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
        System.out.println("[DISPLAY] WARNING: problems reading topic configuration : " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
        System.out.flush();
        System.exit(-1);
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

  private static void handleTopicSetup(String topicsFolderPath)
  {
    //
    // get topics that need to exist
    //

    Map<String, NewTopic> topicsToSetup = new HashMap<>();
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
  
  private static void handleConnectors(String connectorsFilePath)
  {
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
                ObjectHolder<String> existingConfigResponseContent = new ObjectHolder<String>();
                ObjectHolder<Integer> existingConfigHttpResponseCode = new ObjectHolder<Integer>();
                boolean alreadyExist = false;
                try
                  {
                    while(existingConfigHttpResponseCode.getValue() == null || existingConfigHttpResponseCode.getValue() == 409 /* can happen if calls are made too quickly */) {
                      executeCurl(url + "/" + connectorName, null, "-XGET", existingConfigHttpResponseCode, existingConfigResponseContent);
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
                        System.out.println("[DISPLAY] MANDATORY: must update connector config from " + existingConfigResponseContent.getValue() + " to " + jsonConnectorToSetupConfig);

//                        //
//                        // update existing connector with new configuration
//                        //
//                        try
//                          {
//                            ObjectHolder<String> updateResponseContent = new ObjectHolder<String>();
//                            ObjectHolder<Integer> updateResponseCode = new ObjectHolder<Integer>();
//                            executeCurl(url + "/" + connectorName + "/config", JSONUtilities.decodeJSONObject(connectorToSetup, "config").toString(), "-XPUT", updateResponseCode, updateResponseContent);
//                            if (updateResponseCode.getValue().intValue() >= 200 && updateResponseCode.getValue().intValue() < 300)
//                              {
//                                System.out.println("INFO: upgrade of connector " + connectorName + " well executed");
//                              }
//                            else
//                              {
//                                System.out.println("WARNING: Problem while updating connector " + connectorName + " response code " + updateResponseCode.getValue().intValue());
//                              }
//                          }
//                        catch (EvolutionSetupException e)
//                          {
//                            System.out.println("WARNING: Problem while updating connector " + connectorName + " due to Exception " + e.getMessage());
//                            e.printStackTrace();
//                          }
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
                        executeCurl(url + "/" + connectorName + "/config", com.evolving.nglm.core.JSONUtilities.decodeJSONObject(connectorToSetup, "config").toString(), "-XPUT", createResponseCode, createResponseContent);
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
      }
    catch (IOException e)
      {
        System.out.println("[DISPLAY] WARNING: problems creating connectors: " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
        System.out.flush();
        System.exit(-1);
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
  
  private static void executeCurl(String url, String jsonRequestEntity, String httpMethod, ObjectHolder<Integer> httpResponseCode, ObjectHolder<String> responseBody) throws EvolutionSetupException
  {
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

    httpRequest.setConfig(RequestConfig.custom().setConnectTimeout(20).build());
    try
      {
        httpResponse = httpClient.execute(httpRequest);
      }
    catch (IOException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        throw new EvolutionSetupException("[DISPLAY] Exception processing REST api: {}" + stackTraceWriter.toString());
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
            throw new EvolutionSetupException("[DISPLAY] Exception while decoding http response body " + stackTraceWriter.toString());
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

    public CurlCommand(String verb, String url, String jsonBody) {
      this.verb = verb;
      this.url = url;
      this.jsonBody = jsonBody;
    }
  }
  
  private static List<CurlCommand> handleCurlFile(String curlFilePath) 
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

          System.out.println("DEBUG: Handle CURL line " + line);
          String[] params = line.split("\\|\\|\\|");
          
          String verb = null;
          String url = null;
          String jsonBody = null;
          for (int i = 0; i < params.length; i++) {
            if (params[i].trim().equals("-d")) {
              jsonBody = params[i + 2]; // because i+1 refers to a space separator " "
            }
            else if (params[i].trim().startsWith("-d")) { // when there is no space between -d and the argument
              jsonBody = params[i].substring(2).trim();
            }
            else if (params[i].trim().startsWith("http")) {
              url = params[i];
            }
            else if (params[i].trim().startsWith("-X")) {
              verb = params[i];
            }
          }

          System.out.println("DEBUG: VERB="+verb+" URL="+url+" BODY="+jsonBody);
          if (verb == null || url == null || jsonBody == null) {
            System.out.println("[DISPLAY] WARNING: Unable to handle CURL line correctly: " + line);
          } else {
            //
            // parse Deployment calls
            //
            jsonBody = parseJsonBody(jsonBody);
            
            result.add(new CurlCommand(verb, url, jsonBody));
          }

          line = reader.readLine();
        }
      }
    catch (IOException e)
      {
        System.out.println("[DISPLAY] WARNING: Problems in reading CURL line: " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
        System.out.flush();
        System.exit(-1);
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
  
  private static String parseJsonBody(String jsonBody) 
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
          java.lang.reflect.Method getter = Deployment.class.getMethod(call);
          String replace = getter.invoke(null).toString();
          result = result.replaceAll(Pattern.compile("Deployment\\."+call+"\\(\\)").pattern(), replace);
        }
      catch(InvocationTargetException| NoSuchMethodException| IllegalAccessException e)
        {
          System.out.println("[DISPLAY] ERROR: Unable to call Deployment." + call);
          System.out.println(e.getMessage());
        }
    }
    
    System.out.println("DEBUG: JSONBODY="+ result);
    
    return result;
  }
  
}