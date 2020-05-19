/*****************************************************************************
*
*  KafkaMultipleTopicCreate.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaMultipleTopicCreate
{
  /****************************************
  *
  *  main
  *
  ****************************************/

  public static void main(String[] args)
  {
    String line = null;
    try
      {
        BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])));
        Pattern mainPattern = Pattern.compile("^create_topic\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.*)$");
        Pattern optionPattern = Pattern.compile("\\s*(\\S+)=(\\S+)");
        while (true)
          {
            //
            //  get line
            //
            
            line = reader.readLine();
            if (line == null) break;
            
            //
            //  parse line
            //  create_topic <topicName> <partitions> <replicationFactor> --config param1=value1 --config param2=value2
            //
            
            Matcher mainMatcher = mainPattern.matcher(line);
            if (mainMatcher.matches())
              {
                //
                //  required parameters
                //  - topicName
                //  - replicationFactor
                //  - partitions
                //
                
                String topicName = mainMatcher.group(1);
                short replicationFactor = Short.parseShort(mainMatcher.group(2));
                int partitions = Integer.parseInt(mainMatcher.group(3));
                
                //
                //  additional configuration
                //
                
                String additionalParameters = mainMatcher.group(4);
                Map<String,String> configs = new HashMap<>();
                Matcher optionMatcher = optionPattern.matcher(additionalParameters);
                while (optionMatcher.find())
                  {
                    configs.put(optionMatcher.group(1), optionMatcher.group(2));
                  }

                //
                //  new topic
                //
                
                NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor).configs(configs);
                
                //
                //  connection
                //
                
                Properties adminClientConfig = new Properties();
                adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("broker.servers"));
                AdminClient adminClient = AdminClient.create(adminClientConfig);

                //
                //  create topic
                //

                try
                  {
                    CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
                    for (KafkaFuture future : result.values().values()) future.get();
                  }
                catch (InterruptedException|ExecutionException e)
                  {
                    System.out.println("problems creating topic '" + topicName + "': " + e.getMessage());
                  }
              }
            else
              {
                System.out.println("skipping badly-formatted topic configuration line: '" + line + "'");
              }
          }
      }
    catch (IOException e)
      {
        System.out.println("problems creating topics: " + e.getMessage() + ((line != null) ? " (" + line + ")" : ""));
        System.out.flush();
        System.exit(-1);
      }
  }
}