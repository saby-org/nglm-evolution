package com.evolving.nglm.evolution.reports.customerpointdetails;

import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;

public class CustomerPointDetailsESReader
{
  private static final Logger log = LoggerFactory.getLogger(CustomerPointDetailsESReader.class);

  /****************************************
   * 
   * main
   * 
   ****************************************/
  
  public static void main(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("LoyaltyProgramESReader: arg " + arg);
      }

    if (args.length < 5)
      {
        log.warn("Usage : CustomerPointDetailsESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index>");
        return;
      }

    String topicName = args[0];
    String kafkaNodeList = args[1];
    String kzHostList = args[2];
    String esNode = args[3];
    String esIndexSubscriber = args[4];

    log.info("Reading data from ES in " + esIndexSubscriber + " index and writing to " + topicName + " topic.");

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());
    log.info("RAJ K ES indexes to read {}", esIndexWithQuery.keySet());
    
    //
    //  reportEsReader
    //
    
    ReportEsReader reportEsReader = new ReportEsReader("subscriberID", topicName, kafkaNodeList, kzHostList, esNode, esIndexWithQuery, true);
    reportEsReader.start();
    
    log.info("Finished CustomerPointDetailsESReader");
  }
}
