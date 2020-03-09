package com.evolving.nglm.evolution.reports.customerpointdetails;

import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

public class CustomerPointDetailsESReader
{
  private static final Logger log = LoggerFactory.getLogger(CustomerPointDetailsESReader.class);

  public static void main(String[] args) {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("LoyaltyProgramESReader: arg " + arg);
    }

    if (args.length < 5) {
      log.warn(
          "Usage : CustomerPointDetailsESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index>");
      return;
    }

    String topicName       = args[0];
    String kafkaNodeList   = args[1];
    String kzHostList      = args[2];
    String esNode          = args[3];
    String esIndexSubscriber  = args[4];

    log.info("Reading data from ES in "+esIndexSubscriber+" index and writing to "+topicName+" topic.");   

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());

    ReportEsReader reportEsReader = new ReportEsReader(
        "subscriberID",
        topicName,
        kafkaNodeList,
        kzHostList,
        esNode,
        esIndexWithQuery
        );

    reportEsReader.start();
    log.info("Finished CustomerPointDetailsESReader");
  }
}
