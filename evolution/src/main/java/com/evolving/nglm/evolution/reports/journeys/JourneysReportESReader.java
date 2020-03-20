package com.evolving.nglm.evolution.reports.journeys;

import com.evolving.nglm.evolution.reports.journeyimpact.JourneyImpactReportESReader;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

public class JourneysReportESReader
{
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportESReader.class);

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneysReportESReader: arg " + arg);
      }

    if (args.length < 5)
      {
        log.warn("Usage : JourneysReportESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index> <ES journey index>");
        return;
      }

    String topicName = args[0];
    String kafkaNodeList = args[1];
    String kzHostList = args[2];
    String esNode = args[3];
    String esIndexJourney = args[4];

    log.info("Reading data from ES in " + esIndexJourney + " index and writing to " + topicName + " topic.");

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexJourney, QueryBuilders.matchAllQuery());

    ReportEsReader reportEsReader = new ReportEsReader("journeyID", topicName, kafkaNodeList, kzHostList, esNode, esIndexWithQuery);

    reportEsReader.start();
    log.info("Finished JourneysReportESReader");
  }
}